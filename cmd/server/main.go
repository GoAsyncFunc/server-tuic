package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v2"

	"github.com/GoAsyncFunc/server-tuic/internal/app/server"

	"github.com/GoAsyncFunc/server-tuic/internal/pkg/service"
	api "github.com/GoAsyncFunc/uniproxy/pkg"
)

const (
	Name      = "tuic-node"
	Version   = "0.0.1"
	CopyRight = "GoAsyncFunc@2025"
)

func main() {
	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Printf("%s version %s\n", c.App.Name, c.App.Version)
	}

	var config server.Config
	var apiConfig api.Config
	var serviceConfig service.Config
	var certConfig service.CertConfig
	var extConfPath string
	var dataDir string

	app := &cli.App{
		Name:      Name,
		Version:   Version,
		Copyright: CopyRight,
		Usage:     "Provide TUIC service for V2Board",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "api",
				Usage:       "Server address",
				EnvVars:     []string{"API"},
				Required:    true,
				Destination: &apiConfig.APIHost,
			},
			&cli.StringFlag{
				Name:        "token",
				Usage:       "Token of server API",
				EnvVars:     []string{"TOKEN"},
				Required:    true,
				Destination: &apiConfig.Key,
			},
			&cli.StringFlag{
				Name:        "ext_conf_file",
				Usage:       "Extended profiles (ignored in TUIC)",
				EnvVars:     []string{"EXT_CONF_FILE"},
				Required:    false,
				Destination: &extConfPath,
			},
			&cli.StringFlag{
				Name:        "cert_file",
				Usage:       "Cert file",
				EnvVars:     []string{"CERT_FILE"},
				Value:       "/root/.cert/server.crt",
				Required:    false,
				DefaultText: "/root/.cert/server.crt",
				Destination: &certConfig.CertFile,
			},
			&cli.StringFlag{
				Name:        "key_file",
				Usage:       "Key file",
				EnvVars:     []string{"KEY_FILE"},
				Value:       "/root/.cert/server.key",
				Required:    false,
				DefaultText: "/root/.cert/server.key",
				Destination: &certConfig.KeyFile,
			},
			&cli.IntFlag{
				Name:        "node",
				Usage:       "Node ID",
				EnvVars:     []string{"NODE"},
				Required:    true,
				Destination: &apiConfig.NodeID,
			},
			&cli.DurationFlag{
				Name:        "fetch_users_interval, fui",
				Usage:       "API request cycle(fetch users), unit: second",
				EnvVars:     []string{"FETCH_USER_INTERVAL"},
				Value:       time.Second * 60,
				DefaultText: "60",
				Required:    false,
				Destination: &serviceConfig.FetchUsersInterval,
			},
			&cli.DurationFlag{
				Name:        "report_traffics_interval, fui",
				Usage:       "API request cycle(report traffics), unit: second",
				EnvVars:     []string{"REPORT_TRAFFICS_INTERVAL"},
				Value:       time.Second * 80,
				DefaultText: "80",
				Required:    false,
				Destination: &serviceConfig.ReportTrafficsInterval,
			},
			&cli.DurationFlag{
				Name:        "heartbeat_interval, hbi",
				Usage:       "API request cycle(heartbeat), unit: second",
				EnvVars:     []string{"HEARTBEAT_INTERVAL"},
				Value:       time.Minute * 3,
				DefaultText: "180",
				Required:    false,
				Destination: &serviceConfig.HeartbeatInterval,
			},
			&cli.StringFlag{
				Name:        "log_mode",
				Value:       server.LogLevelError,
				Usage:       "Log mode",
				EnvVars:     []string{"LOG_LEVEL"},
				Destination: &config.LogLevel,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "data_dir",
				Usage:       "Data directory",
				EnvVars:     []string{"DATA_DIR"},
				Value:       "/var/lib/tuic-node",
				DefaultText: "/var/lib/tuic-node",
				Required:    false,
				Destination: &dataDir,
			},
		},
		Before: func(c *cli.Context) error {
			log.SetFormatter(&log.TextFormatter{})
			switch config.LogLevel {
			case server.LogLevelDebug:
				log.SetFormatter(&log.TextFormatter{
					FullTimestamp: true,
				})
				log.SetLevel(log.DebugLevel)
				log.SetReportCaller(true)
			case server.LogLevelInfo:
				log.SetLevel(log.InfoLevel)
			case server.LogLevelError:
				log.SetLevel(log.ErrorLevel)
			default:
				return fmt.Errorf("log mode %s not supported", config.LogLevel)
			}
			return nil
		},
		Action: func(c *cli.Context) error {
			serviceConfig.Cert = &certConfig
			var extFileBytes []byte

			apiConfig.NodeType = api.Tuic

			serv, err := server.New(&config, &apiConfig, &serviceConfig, extFileBytes, dataDir)
			if err != nil {
				return fmt.Errorf("failed to create server: %w", err)
			}
			if err := serv.Start(); err != nil {
				serv.Close()
				return fmt.Errorf("failed to start server: %w", err)
			}

			defer func() {
				if e := recover(); e != nil {
					log.Errorf("panic: %v", e)
					buf := make([]byte, 4096)
					n := runtime.Stack(buf, false)
					log.Errorf("stack trace:\n%s", buf[:n])
					serv.Close()
					os.Exit(1)
				} else {
					serv.Close()
				}
			}()

			runtime.GC()
			{
				osSignals := make(chan os.Signal, 1)
				signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)
				<-osSignals
			}
			return nil
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:    "version",
			Aliases: []string{"v"},
			Usage:   "Show version information",
			Action: func(c *cli.Context) error {
				fmt.Printf("version=%s\n", Version)
				return nil
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
