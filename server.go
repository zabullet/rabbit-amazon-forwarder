package main

import (
	"context"
	"net/http"
	"os"

	"github.com/AirHelp/rabbit-amazon-forwarder/mapping"
	"github.com/AirHelp/rabbit-amazon-forwarder/supervisor"
	log "github.com/sirupsen/logrus"
)

// LogLevel is the log level
const (
	LogLevel = "LOG_LEVEL"
)

func main() {
	createLogger()

	mux := http.NewServeMux()
	server := http.Server{Addr: ":8080", Handler: mux}

	consumerForwarderMap, err := mapping.New().Load()
	if err != nil {
		log.WithField("error", err.Error()).Fatalf("Could not load consumer - forwarder pairs")
	}
	supervisor := supervisor.New(consumerForwarderMap)
	if err := supervisor.Start(); err != nil {
		log.WithField("error", err.Error()).Fatal("Could not start supervisor")
	}
	mux.HandleFunc("/restart", supervisor.Restart)
	mux.HandleFunc("/health", supervisor.Check)
	mux.HandleFunc("/stop", supervisor.Stop)
	mux.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		server.Shutdown(context.Background())
	})

	log.Info("Starting http server")
	log.Fatal(server.ListenAndServe())

}

func createLogger() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
	if logLevel := os.Getenv(LogLevel); logLevel != "" {
		if level, err := log.ParseLevel(logLevel); err != nil {
			log.Fatal(err)
		} else {
			log.SetLevel(level)
		}
	}
}
