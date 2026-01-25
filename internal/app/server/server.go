package server

import (
	"context"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/GoAsyncFunc/server-tuic/internal/pkg/service"
	api "github.com/GoAsyncFunc/uniproxy/pkg"
)

type Config struct {
	LogLevel string
}

const (
	LogLevelDebug  = "debug"
	LogLevelInfo   = "info"
	LogLevelError  = "error"
	DefaultDataDir = "/var/lib/tuic-node"
)

type Server struct {
	logLevel      string
	serviceConfig *service.Config
	apiClient     *api.Client
	config        *Config
	extConfBytes  []byte
	service       *service.Builder
	mu            sync.Mutex
	dataDir       string
	ctx           context.Context
	cancel        context.CancelFunc
}

func New(config *Config, apiConfig *api.Config, serviceConfig *service.Config, extConfBytes []byte, dataDir string) (*Server, error) {
	// API Client initialization
	client := api.New(apiConfig)
	if dataDir == "" {
		dataDir = DefaultDataDir
	}
	return &Server{
		config:        config,
		logLevel:      config.LogLevel,
		apiClient:     client,
		serviceConfig: serviceConfig,
		extConfBytes:  extConfBytes,
		dataDir:       dataDir,
	}, nil
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Configure logrus level
	if lvl, err := log.ParseLevel(s.config.LogLevel); err == nil {
		log.SetLevel(lvl)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.Infoln("server start")

	s.ctx, s.cancel = context.WithCancel(context.Background())
	ctx := s.ctx

	// Fetch node config
	nodeConfig, err := s.apiClient.GetNodeInfo(ctx)
	if err != nil {
		return fmt.Errorf("get node info error: %s", err)
	}
	if nodeConfig == nil {
		return fmt.Errorf("node info is empty (or 304 Not Modified on first start)")
	}

	s.serviceConfig.NodeID = nodeConfig.Id

	s.service = service.New(
		s.ctx,
		s.serviceConfig,
		nodeConfig,
		s.apiClient,
	)

	if err := s.service.Start(); err != nil {
		return fmt.Errorf("start service error: %s", err)
	}

	log.Infof("Server started")
	return nil
}

func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancel != nil {
		s.cancel()
	}

	if s.service != nil {
		err := s.service.Close()
		if err != nil {
			log.Errorf("server close failed: %s", err)
		}
	}
	log.Infoln("server close")
}
