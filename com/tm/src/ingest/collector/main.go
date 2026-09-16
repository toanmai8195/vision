// Command event_collector nhận DataEvent qua HTTP và (từ P2) đẩy vào Kafka.
package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/encoding/protojson"

	eventv1 "com.tm/vision/com/tm/proto/vision/event/v1"
)

const maxBodyBytes = 1 << 20

var eventsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "vision_collector_events_total",
		Help: "Số DataEvent nhận được, theo loại payload và kết quả.",
	},
	[]string{"payload", "result"},
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := os.Getenv("COLLECTOR_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(eventsTotal)

	srv := &http.Server{
		Addr:              addr,
		Handler:           newMux(logger, registry),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("event_collector listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown failed", "err", err)
	}
}

func newMux(logger *slog.Logger, registry *prometheus.Registry) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) {
		handleEvent(logger, w, r)
	})
	return mux
}

func handleEvent(logger *slog.Logger, w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		eventsTotal.WithLabelValues("unknown", "read_error").Inc()
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}

	ev := &eventv1.DataEvent{}
	if err := protojson.Unmarshal(body, ev); err != nil {
		eventsTotal.WithLabelValues("unknown", "invalid_json").Inc()
		http.Error(w, "invalid DataEvent json: "+err.Error(), http.StatusBadRequest)
		return
	}

	payload := payloadLabel(ev)
	if err := Validate(ev); err != nil {
		eventsTotal.WithLabelValues(payload, "invalid").Inc()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// TODO(P2): đối chiếu catalog (PayloadMatchesDataType) và produce vào Kafka.
	eventsTotal.WithLabelValues(payload, "accepted").Inc()
	logger.Debug("event accepted", "event_id", ev.GetEventId(), "attr", ev.GetAttr())
	w.WriteHeader(http.StatusAccepted)
}

func payloadLabel(ev *eventv1.DataEvent) string {
	switch ev.GetPayload().(type) {
	case *eventv1.DataEvent_Tag:
		return "tag"
	case *eventv1.DataEvent_Value:
		return "value"
	case *eventv1.DataEvent_TagValue:
		return "tag_value"
	default:
		return "none"
	}
}
