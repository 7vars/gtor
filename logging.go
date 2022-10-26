package gtor

import "github.com/sirupsen/logrus"

type Logger interface {
	WithField(string, interface{}) Logger
	With(map[string]interface{}) Logger

	Debugf(string, ...interface{})
	Infof(string, ...interface{})
	Warnf(string, ...interface{})
	Errorf(string, ...interface{})
	Fatalf(string, ...interface{})
	Panicf(string, ...interface{})

	Debug(...interface{})
	Info(...interface{})
	Warn(...interface{})
	Error(...interface{})
	Fatal(...interface{})
	Panic(...interface{})

	Debugln(...interface{})
	Infoln(...interface{})
	Warnln(...interface{})
	Errorln(...interface{})
	Fatalln(...interface{})
	Panicln(...interface{})
}

func newLogger() Logger {
	return &logrusLoggerWrapper{
		logrus.StandardLogger(),
	}
}

type logrusLoggerWrapper struct {
	*logrus.Logger
}

func (l *logrusLoggerWrapper) WithField(field string, value interface{}) Logger {
	return &logrusEntryWrapper{l.Logger.WithField(field, value)}
}

func (l *logrusLoggerWrapper) With(fields map[string]interface{}) Logger {
	return &logrusEntryWrapper{l.Logger.WithFields(fields)}
}

type logrusEntryWrapper struct {
	*logrus.Entry
}

func (e *logrusEntryWrapper) WithField(field string, value interface{}) Logger {
	return &logrusEntryWrapper{e.Entry.WithField(field, value)}
}

func (e *logrusEntryWrapper) With(fields map[string]interface{}) Logger {
	return &logrusEntryWrapper{e.Entry.Logger.WithFields(fields)}
}

func init() {
	conf := config()

	switch conf.GetStringDefault("gtor.log.level", "DEBUG") {
	case "DEBUG":
		logrus.SetLevel(logrus.DebugLevel)
	case "WARN":
		logrus.SetLevel(logrus.WarnLevel)
	case "ERROR":
		logrus.SetLevel(logrus.ErrorLevel)
	case "FATAL":
		logrus.SetLevel(logrus.FatalLevel)
	case "PANIC":
		logrus.SetLevel(logrus.PanicLevel)
	default:
		logrus.SetLevel(logrus.InfoLevel)
	}

	switch conf.GetStringDefault("gtor.log.formatter", "text") {
	case "json":
		logrus.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
		})
	default:
		logrus.SetFormatter(&logrus.TextFormatter{
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
			FullTimestamp:   true,
		})
	}
}
