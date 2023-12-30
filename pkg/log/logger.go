package log

type logger struct {
	prefix string
}

type Logger interface {
	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	LogIfError(err error, msg string)
	Log(err error, format string, v ...interface{})
}

func WithLogger(prefix string) Logger {
	return &logger{
		prefix: prefix,
	}
}

func (l *logger) WithPrefix(prefix string) Logger {
	return &logger{
		prefix: l.prefix + prefix,
	}
}

func (l *logger) Panic(v ...interface{}) {
	sugar.Panic(v...)
}

func (l *logger) Panicf(format string, v ...interface{}) {
	sugar.Panicf(l.prefix+format, v...)
}

func (l *logger) Errorf(format string, v ...interface{}) {
	sugar.Errorf(l.prefix+format, v...)
}

func (l *logger) Warnf(format string, v ...interface{}) {
	sugar.Warnf(l.prefix+format, v...)
}

func (l *logger) Infof(format string, v ...interface{}) {
	sugar.Infof(l.prefix+format, v...)
}

func (l *logger) Debugf(format string, v ...interface{}) {
	sugar.Debugf(l.prefix+format, v...)
}

func (l *logger) LogIfError(err error, msg string) {
	if err == nil {
		return
	}
	Error(l.prefix+msg, err)
}

func (l *logger) Log(err error, format string, v ...interface{}) {
	if err == nil {
		l.Infof(format, v...)
	} else {
		l.Errorf(format, v...)
	}
}
