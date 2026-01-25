package service

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	api "github.com/GoAsyncFunc/uniproxy/pkg"
	"github.com/sagernet/sing-quic/tuic"
	"github.com/sagernet/sing/common/auth"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	aTLS "github.com/sagernet/sing/common/tls"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	NodeID                 int
	NodeType               string
	FetchUsersInterval     time.Duration
	ReportTrafficsInterval time.Duration
	HeartbeatInterval      time.Duration
	Cert                   *CertConfig
	ListenAddr             string
}

type CertConfig struct {
	CertFile string
	KeyFile  string
}

type Builder struct {
	config    *Config
	nodeInfo  *api.NodeInfo
	apiClient *api.Client

	service *tuic.Service[int]

	// Traffic Stats
	trafficStats *TrafficStats

	// Users
	userList []api.UserInfo

	mu sync.Mutex

	fetchUsersMonitorPeriodic      *Periodic
	reportTrafficsMonitorPeriodic  *Periodic
	heartbeatMonitorPeriodic       *Periodic
	checkNodeConfigMonitorPeriodic *Periodic

	ctx    context.Context
	cancel context.CancelFunc
}

// Simple Periodic task wrapper (copied)
type Periodic struct {
	Interval time.Duration
	Execute  func() error
	stop     chan struct{}
}

func (p *Periodic) Start() error {
	p.stop = make(chan struct{})
	go func() {
		ticker := time.NewTicker(p.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := p.Execute(); err != nil {
					log.Errorf("Periodic task error: %v", err)
				}
			case <-p.stop:
				return
			}
		}
	}()
	return nil
}

func (p *Periodic) Close() {
	if p.stop != nil {
		close(p.stop)
	}
}

func New(ctx context.Context, config *Config, nodeInfo *api.NodeInfo, apiClient *api.Client) *Builder {
	ctx, cancel := context.WithCancel(ctx)
	return &Builder{
		config:       config,
		nodeInfo:     nodeInfo,
		apiClient:    apiClient,
		ctx:          ctx,
		cancel:       cancel,
		trafficStats: NewTrafficStats(),
	}
}

func (b *Builder) Start() error {
	if err := b.startTuic(); err != nil {
		return err
	}

	// Initial user fetch
	userList, err := b.apiClient.GetUserList(b.ctx)
	if err != nil {
		return err
	}
	b.updateUsers(userList)
	b.userList = userList

	b.fetchUsersMonitorPeriodic = &Periodic{
		Interval: b.config.FetchUsersInterval,
		Execute:  b.fetchUsersMonitor,
	}
	b.reportTrafficsMonitorPeriodic = &Periodic{
		Interval: b.config.ReportTrafficsInterval,
		Execute:  b.reportTrafficsMonitor,
	}

	log.Infoln("Start monitoring for user acquisition")
	if err := b.fetchUsersMonitorPeriodic.Start(); err != nil {
		return err
	}

	log.Infoln("Start traffic reporting monitoring")
	if err := b.reportTrafficsMonitorPeriodic.Start(); err != nil {
		return err
	}

	return nil
}

func (b *Builder) startTuic() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.startTuicInternal()
}

func (b *Builder) startTuicInternal() error {
	if b.service != nil {
		b.service.Close()
		b.service = nil
	}

	tuicInfo := b.nodeInfo.Tuic
	if tuicInfo == nil {
		return fmt.Errorf("node info missing Tuic config")
	}

	cert, err := tls.LoadX509KeyPair(b.config.Cert.CertFile, b.config.Cert.KeyFile)
	if err != nil {
		return fmt.Errorf("failed to load TLS config: %w", err)
	}

	// Listen
	listenAddr := fmt.Sprintf(":%d", tuicInfo.ServerPort)
	conn, err := net.ListenPacket("udp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen UDP: %w", err)
	}

	handler := &Handler{
		trafficStats: b.trafficStats,
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"tuic", "tuic-v5", "h3"},
	}

	options := tuic.ServiceOptions{
		Context:           b.ctx,
		Logger:            &TuicLogger{},
		TLSConfig:         &MyTLSConfig{config: tlsConfig},
		CongestionControl: tuicInfo.CongestionControl,
		ZeroRTTHandshake:  tuicInfo.ZeroRTTHandshake,
		Handler:           handler,
	}

	service, err := tuic.NewService[int](options)
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to create TUIC service: %w", err)
	}
	b.service = service

	go func() {
		log.Infof("TUIC server starting on %s", listenAddr)
		if err := service.Start(conn); err != nil {
			log.Errorf("TUIC server exited with error: %v", err)
		}
	}()

	return nil
}

func (b *Builder) Close() error {
	b.cancel()
	if b.fetchUsersMonitorPeriodic != nil {
		b.fetchUsersMonitorPeriodic.Close()
	}
	if b.reportTrafficsMonitorPeriodic != nil {
		b.reportTrafficsMonitorPeriodic.Close()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.service != nil {
		b.service.Close()
		b.service = nil
	}
	return nil
}

func (b *Builder) fetchUsersMonitor() error {
	newUserList, err := b.apiClient.GetUserList(b.ctx)
	if err != nil {
		log.Errorln(err)
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.updateUsers(newUserList)
	b.userList = newUserList
	return nil
}

func (b *Builder) reportTrafficsMonitor() error {
	stats := b.trafficStats.GetAndReset()
	if len(stats) == 0 {
		return nil
	}

	userTraffic := make([]api.UserTraffic, 0, len(stats))
	for uidStr, s := range stats {
		var uid int
		fmt.Sscanf(uidStr, "%d", &uid)
		if uid > 0 && (s.Tx > 0 || s.Rx > 0) {
			userTraffic = append(userTraffic, api.UserTraffic{
				UID:      uid,
				Upload:   int64(s.Tx),
				Download: int64(s.Rx),
			})
		}
	}

	if len(userTraffic) > 0 {
		log.Infof("%d user traffic needs to be reported", len(userTraffic))
		err := b.apiClient.ReportUserTraffic(b.ctx, userTraffic)
		if err != nil {
			log.Errorln("server error when submitting traffic", err)
			return nil
		}
	}
	return nil
}

func (b *Builder) updateUsers(users []api.UserInfo) {
	if b.service == nil {
		return
	}
	var ids []int
	var uuids [][16]byte
	var passwords []string

	for _, u := range users {
		ids = append(ids, u.Id)
		// Parse UUID
		var uuid [16]byte
		if parsed, err := parseUUID(u.Uuid); err == nil {
			uuid = parsed
		}
		uuids = append(uuids, uuid)
		passwords = append(passwords, u.Uuid)
	}
	b.service.UpdateUsers(ids, uuids, passwords)
	log.Debugf("Updated %d users", len(users))
}

func parseUUID(s string) ([16]byte, error) {
	var uuid [16]byte
	s = strings.ReplaceAll(s, "-", "")
	b, err := hex.DecodeString(s)
	if err != nil {
		return uuid, err
	}
	copy(uuid[:], b)
	return uuid, nil
}

// MyTLSConfig implements aTLS.ServerConfig
type MyTLSConfig struct {
	config *tls.Config
}

var _ aTLS.ServerConfig = (*MyTLSConfig)(nil)

func (c *MyTLSConfig) Config() (*tls.Config, error) { return c.config, nil }
func (c *MyTLSConfig) ServerName() string           { return c.config.ServerName }
func (c *MyTLSConfig) SetServerName(s string)       { c.config.ServerName = s }
func (c *MyTLSConfig) NextProtos() []string         { return c.config.NextProtos }
func (c *MyTLSConfig) SetNextProtos(s []string)     { c.config.NextProtos = s }
func (c *MyTLSConfig) Client(conn net.Conn) (aTLS.Conn, error) {
	return nil, fmt.Errorf("not client")
}
func (c *MyTLSConfig) Clone() aTLS.Config {
	return &MyTLSConfig{config: c.config.Clone()}
}
func (c *MyTLSConfig) Start() error { return nil }
func (c *MyTLSConfig) Close() error { return nil }
func (c *MyTLSConfig) Server(conn net.Conn) (aTLS.Conn, error) {
	return aTLS.ServerHandshake(context.Background(), conn, c)
}

// Handler implements ServiceHandler
type Handler struct {
	trafficStats *TrafficStats
}

func (h *Handler) NewConnectionEx(ctx context.Context, conn net.Conn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	destNet := "tcp"
	destAddr := destination.String()

	outConn, err := net.DialTimeout(destNet, destAddr, time.Second*10)
	if err != nil {
		log.Warnf("Failed to dial %s: %v", destAddr, err)
		conn.Close()
		return
	}

	// Wrap connection for traffic logging
	uid, ok := auth.UserFromContext[int](ctx)
	if ok && h.trafficStats != nil {
		conn = &TrafficConn{
			Conn: conn,
			log: func(tx, rx uint64) {
				h.trafficStats.LogTraffic(fmt.Sprintf("%d", uid), tx, rx)
			},
		}
	}

	// Copy
	go func() {
		io.Copy(conn, outConn)
		outConn.Close()
		conn.Close()
	}()
	go func() {
		io.Copy(outConn, conn)
		outConn.Close()
		conn.Close()
	}()
}

func (h *Handler) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	conn.Close()
}

// TrafficStats and TrafficConn

type TrafficStats struct {
	stats map[string]*ConnStats // auth_id (uid as string) -> stats
	mu    sync.Mutex
}

type ConnStats struct {
	Tx uint64
	Rx uint64
}

func NewTrafficStats() *TrafficStats {
	return &TrafficStats{
		stats: make(map[string]*ConnStats),
	}
}

func (s *TrafficStats) LogTraffic(id string, tx, rx uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.stats[id]; !ok {
		s.stats[id] = &ConnStats{}
	}
	s.stats[id].Tx += tx
	s.stats[id].Rx += rx
	return true
}

func (s *TrafficStats) GetAndReset() map[string]*ConnStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.stats
	s.stats = make(map[string]*ConnStats)
	return r
}

type TrafficConn struct {
	net.Conn
	log func(tx, rx uint64)
}

func (c *TrafficConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.log(0, uint64(n))
	}
	return n, err
}

func (c *TrafficConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 {
		c.log(uint64(n), 0)
	}
	return n, err
}
