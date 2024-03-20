package log

import (
	"errors"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/mgtv-tech/redis-GunYu/config"
)

var (
	glogger *zap.Logger
	sugar   *zap.SugaredLogger
)

func init() {
	glogger, _ = zap.NewProduction()
	sugar = glogger.Sugar()
}

var (
	ErrNoHandler = errors.New("no handler")
)

func InitLog(cfg config.LogConfig) error {
	//return nil
	var err error
	syncers := []zapcore.WriteSyncer{}
	if cfg.Hander.File != nil {
		syncers = append(syncers, zapcore.AddSync(&lumberjack.Logger{
			Filename:   cfg.Hander.File.FileName,
			MaxSize:    cfg.Hander.File.MaxSize,
			MaxBackups: cfg.Hander.File.MaxBackups,
			MaxAge:     cfg.Hander.File.MaxAge,
		}))
	}
	if cfg.Hander.StdOut {
		syncers = append(syncers, zapcore.AddSync(zapcore.Lock(os.Stdout)))
	}
	if len(syncers) == 0 {
		return ErrNoHandler
	}

	level := zapcore.InfoLevel
	stLevel := zapcore.PanicLevel
	if len(cfg.LevelStr) > 0 {
		level, err = zapcore.ParseLevel(cfg.LevelStr)
		if err != nil {
			return err
		}
	}
	if len(cfg.StacktraceLevelStr) > 0 {
		stLevel, err = zapcore.ParseLevel(cfg.StacktraceLevelStr)
		if err != nil {
			return err
		}
	}

	w := zapcore.NewMultiWriteSyncer(syncers...)

	encodeCfg := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.RFC3339NanoTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		//EncodeName:     zapcore.FullNameEncoder,
	}
	if *cfg.Caller {
		encodeCfg.CallerKey = "caller"
	}
	if *cfg.Func {
		encodeCfg.FunctionKey = "func"
	}

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encodeCfg),
		//zapcore.NewJSONEncoder(encodeCfg),
		w,
		level,
	)
	glogger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1), zap.AddStacktrace(stLevel))
	sugar = glogger.Sugar()

	return nil
}

func Sync() error {
	return glogger.Sync()
}

func Panic(v ...interface{}) {
	sugar.Panic(v...)
}

func Panicf(format string, v ...interface{}) {
	sugar.Panicf(format, v...)
}

func PanicError(err error, v ...interface{}) {
	sugar.Panic(err, v)
}

func Error(v ...interface{}) {
	sugar.Error(v...)
}

func Errorf(format string, v ...interface{}) {
	sugar.Errorf(format, v...)
}

func Warn(v ...interface{}) {
	sugar.Warn(v...)
}

func Warnf(format string, v ...interface{}) {
	sugar.Warnf(format, v...)
}

func Info(v ...interface{}) {
	sugar.Info(v...)
}

func Infof(format string, v ...interface{}) {
	sugar.Infof(format, v...)
}

func Debug(v ...interface{}) {
	sugar.Debug(v...)
}

func Debugf(format string, v ...interface{}) {
	sugar.Debugf(format, v...)
}

func LogIfError(err error, msg string) {
	if err == nil {
		return
	}
	Error(msg, err)
}
