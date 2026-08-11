package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wb_orders_http_requests_total",
		Help: "Total HTTP requests.",
	}, []string{"method", "route", "status"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wb_orders_http_request_duration_seconds",
		Help:    "HTTP request duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})

	OutboxPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wb_orders_outbox_publish_total",
		Help: "Outbox publish outcomes.",
	}, []string{"result"})

	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wb_orders_outbox_pending",
		Help: "Current unpublished outbox events.",
	})

	KafkaProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wb_orders_kafka_messages_total",
		Help: "Kafka processing outcomes.",
	}, []string{"topic", "result"})

	OrderProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "wb_orders_order_processing_duration_seconds",
		Help:    "Order processing duration.",
		Buckets: prometheus.DefBuckets,
	})
)
