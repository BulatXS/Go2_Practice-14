package logger

import (
	"os"

	"github.com/sirupsen/logrus"
)

func New(serviceName string) *logrus.Logger {
	log := logrus.New()
	log.SetOutput(os.Stdout)
	log.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
	})
	log.SetLevel(logrus.InfoLevel)

	return log
}

func WithService(log *logrus.Logger, serviceName string) *logrus.Entry {
	return log.WithField("service", serviceName)
}
