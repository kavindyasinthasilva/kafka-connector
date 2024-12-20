package log

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	tContext "github.com/pickme-go/traceable-context"
	"log"
	"runtime"
)

type logMessage struct {
	typ     string
	color   string
	message interface{}
	uuid    string
	file    string
	line    int
}

type logParser struct {
	*logOptions
	log *log.Logger
}

// isLoggable Check whether the log type is loggable under current configurations
func (l *logParser) isLoggable(level Level) bool {
	return logTypes[level] <= logTypes[l.logLevel]
}

func (l *logParser) colored(level Level) string {
	if l.colors {
		return string(logColors[level])
	}

	return string(level)
}

func (l *logParser) WithPrefix(p string, message interface{}) string {
	if l.prefix != `` {
		if p == `` {
			return fmt.Sprintf(`%s] [%+v`, l.prefix, message)
		}
		return fmt.Sprintf(`%s.%s] [%+v`, l.prefix, p, message)
	}
	return fmt.Sprintf(`%s] [%+v`, p, message)
}

func WithPrefix(p string, message interface{}) string {
	return fmt.Sprintf(`%s] [%+v`, p, message)
}

func uuidFromContext(ctx context.Context) uuid.UUID {
	uid := tContext.FromContext(ctx)
	if uid == uuid.Nil {
		return uuid.New()
	}

	return uid
}

func (l *logParser) logEntry(level Level, ctx context.Context, message interface{}, prms ...interface{}) {
	if !l.isLoggable(level) {
		return
	}

	format := "%s [%s] [%+v]"

	var params []interface{}

	logLevel := string(level)

	if l.colors {
		logLevel = l.colored(level)
	}

	var uid uuid.UUID
	if ctx != nil {
		uid = uuidFromContext(ctx)
	} else {
		uid = uuid.New()
	}

	logMsg := &logMessage{
		typ:     logLevel,
		message: message,
		uuid:    uid.String(),
	}

	params = append(params, logLevel, uid.String(), fmt.Sprintf(`%s`, message))

	funcName := ``
	file := `<Unknown>`
	line := 1
	pc, file, line, ok := runtime.Caller(l.fileDepth)
	if ok {
		funcName = runtime.FuncForPC(pc).Name()
	}

	format = "%s [%s] [%+v" + fmt.Sprintf(` on func %s`, funcName) + "]"

	if l.filePath {
		logMsg.file = file
		logMsg.line = line
		format = "%s [%s] [%+v" + fmt.Sprintf(` on func %s on %s line %d`, funcName, file, line) + "]"
	}

	if len(prms) > 0 {
		format += " %+v"
		params = append(params, prms)
	}

	if level == FATAL {
		l.log.Fatalf(format, params...)
	}

	l.log.Printf(format, params...)
}
