// Copyright (c) 2018, Postgres Professional

package hplog

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	*zap.SugaredLogger
	level zap.AtomicLevel
}

func GetLogger() *Logger {
	level := zap.NewAtomicLevelAt(zapcore.DebugLevel)
	config := zap.Config{
		Level: level,
		// well, don't panic in DPanic and something else
		Development: false,
		// print file name and line always
		DisableCaller: false,
		// never print stacktrace
		DisableStacktrace: true,
		// plain text logging
		Encoding:      "console",
		EncoderConfig: zap.NewDevelopmentEncoderConfig(),
		OutputPaths:   []string{"stderr"},
		// for logger errors itself
		ErrorOutputPaths: []string{"stderr"},
	}

	zlogger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	zslogger := zlogger.Sugar()
	return &Logger{SugaredLogger: zslogger, level: level}
}

func (l *Logger) SetLevel(level string) {
	switch level {
	case "error":
		l.level.SetLevel(zap.ErrorLevel)
	case "warn":
		l.level.SetLevel(zap.WarnLevel)
	case "info":
		l.level.SetLevel(zap.InfoLevel)
	case "debug":
		l.level.SetLevel(zap.DebugLevel)
	default:
		l.Fatalf("invalid log level: %v", level)
	}
}

func GetLoggerWithLevel(level string) *Logger {
	l := GetLogger()
	l.SetLevel(level)
	return l
}
