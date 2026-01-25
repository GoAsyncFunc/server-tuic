package service

import (
	log "github.com/sirupsen/logrus"

	"github.com/sagernet/sing/common/logger"
)

type TuicLogger struct{}

var _ logger.Logger = (*TuicLogger)(nil)

func (l *TuicLogger) Trace(args ...any) {
	log.Trace(args...)
}

func (l *TuicLogger) Debug(args ...any) {
	log.Debug(args...)
}

func (l *TuicLogger) Info(args ...any) {
	log.Info(args...)
}

func (l *TuicLogger) Warn(args ...any) {
	log.Warn(args...)
}

func (l *TuicLogger) Error(args ...any) {
	log.Error(args...)
}

func (l *TuicLogger) Fatal(args ...any) {
	log.Fatal(args...)
}

func (l *TuicLogger) Panic(args ...any) {
	log.Panic(args...)
}
