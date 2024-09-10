package log

import (
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	*zap.Logger
}

var atomicLevel = zap.NewAtomicLevel()

func LoggerInit(level string) (*Logger, error) {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:    "time",
		LevelKey:   "level",
		NameKey:    "logger",
		CallerKey:  "caller",
		MessageKey: "msg",
		//StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder, // 小写编码器
		EncodeTime:     zapcore.ISO8601TimeEncoder,    // ISO8601 UTC 时间格式
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder, // 全路径编码器
	}

	config := zap.Config{
		Level:            atomicLevel,        // 日志级别
		Development:      false,              // 开发模式，堆栈跟踪
		Encoding:         "json",             // 输出格式 console 或 json
		EncoderConfig:    encoderConfig,      // 编码器配置
		OutputPaths:      []string{"stdout"}, // 输出到指定文件 stdout（标准输出，正常颜色） stderr（错误输出，红色）
		ErrorOutputPaths: []string{"stderr"},
	}

	// 构建日志
	log, err := config.Build()
	if err != nil {
		return nil, err
	}

	l := &Logger{log}

	l.Level(level)

	return l, nil
}

func (l *Logger) Debug(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}

	zapFields := newFields(fields)

	if ce := l.Logger.Check(zap.DebugLevel, msg); ce != nil {
		ce.Write(zapFields...)
	}
}

func (l *Logger) Info(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}

	zapFields := newFields(fields)

	if ce := l.Logger.Check(zap.InfoLevel, msg); ce != nil {
		ce.Write(zapFields...)
	}
}

func (l *Logger) Warning(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}

	zapFields := newFields(fields)

	if ce := l.Logger.Check(zap.WarnLevel, msg); ce != nil {
		ce.Write(zapFields...)
	}
}

func (l *Logger) Error(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}

	zapFields := newFields(fields)

	if ce := l.Logger.Check(zap.ErrorLevel, msg); ce != nil {
		ce.Write(zapFields...)
	}
}

func (l *Logger) Fatal(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}

	zapFields := newFields(fields)

	if ce := l.Logger.Check(zap.FatalLevel, msg); ce != nil {
		ce.Write(zapFields...)
	}
}

func newFields(fields map[string]interface{}) []zap.Field {
	// 将 map 转换为 zap.Field 列表
	zapFields := make([]zap.Field, 0, len(fields))
	for key, value := range fields {
		zapFields = append(zapFields, zap.Any(key, value))
	}

	return zapFields
}

func (l *Logger) Level(level string) {
	switch strings.ToLower(level) {
	case "debug":
		atomicLevel.SetLevel(zap.DebugLevel)
	case "warn":
		atomicLevel.SetLevel(zap.WarnLevel)
	case "error":
		atomicLevel.SetLevel(zap.ErrorLevel)
	case "fatal":
		atomicLevel.SetLevel(zap.FatalLevel)
	default:
		atomicLevel.SetLevel(zap.InfoLevel)
	}
}

// 不支持
func (l *Logger) OutputPath(path string) (err error) {
	return nil
}
