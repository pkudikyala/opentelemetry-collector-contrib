// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver"

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
)

// newCache creates a new cache with the given size.
// If the size is less or equal to 0, it will be set to 1.
// It will never return an error.
func newCache(size int) *lru.Cache[string, float64] {
	if size <= 0 {
		size = 1
	}
	// lru will only return error when the size is less than 0
	cache, _ := lru.New[string, float64](size)
	return cache
}

func newTTLCache[v any](size int, ttl time.Duration) *expirable.LRU[string, v] {
	if size <= 0 {
		size = 1
	}
	cache := expirable.NewLRU[string, v](size, nil, ttl)
	return cache
}

func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	cfg := createDefaultPostgreSQLConfig()
	
	// Add New Relic specific default configurations
	cfg.NewRelicQueryPerformance = NewRelicQueryPerformanceConfig{
		Enabled:                    true,
		SlowQueryThresholdMs:       1000,    // 1 second
		MaxQueriesPerCollection:    100,     // Maximum 100 queries per collection
		QueryMonitoringEnabled:     true,    // Enable query monitoring
		WaitEventMonitoringEnabled: true,    // Enable wait event monitoring
		BlockingSessionsEnabled:    true,    // Enable blocking sessions monitoring
		ExecutionPlanEnabled:       true,    // Enable execution plan monitoring
		QueryPlanCacheSize:         1000,    // Cache up to 1000 plans
		QueryPlanCacheTTL:          time.Hour, // Cache TTL of 1 hour
		IndividualQueryEnabled:     true,    // Enable individual query monitoring
	}
	
	return cfg
}

func createDefaultPostgreSQLConfig() *Config {
	cfg := &Config{
		ControllerConfig: scraperhelper.NewDefaultControllerConfig(),
		AddrConfig: confignet.AddrConfig{
			Transport: confignet.TransportTypeTCP,
			Endpoint:  "localhost:5432",
		},
		ClientConfig: configtls.ClientConfig{
			Insecure: true,
		},
		ConnectionPool: ConnectionPool{},
		TopQueryCollection: TopQueryCollection{
			Enabled:                false,
			MaxRowsPerQuery:        50,
			TopNQuery:              10,
			MaxExplainEachInterval: 5,
			QueryPlanCacheSize:     1000,
			QueryPlanCacheTTL:      24 * time.Hour,
		},
		QuerySampleCollection: QuerySampleCollection{
			Enabled:         false,
			MaxRowsPerQuery: 50,
		},
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}
	cfg.ControllerConfig.CollectionInterval = time.Minute
	return cfg
}

func createMetricsReceiver(
	_ context.Context,
	params receiver.Settings,
	rCfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	cfg := rCfg.(*Config)

	pgScraper, err := newPostgreSQLScraper(params, cfg)
	if err != nil {
		return nil, err
	}

	s, err := scraper.NewMetrics(pgScraper.scrape, scraper.WithShutdown(pgScraper.shutdown))
	if err != nil {
		return nil, err
	}

	return scraperhelper.NewMetricsController(
		&cfg.ControllerConfig, params, consumer,
		scraperhelper.AddScraper(metadata.Type, s),
	)
}

func createLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	rCfg component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	cfg := rCfg.(*Config)

	logReceiver := &logsReceiver{
		settings: params,
		config:   cfg,
		consumer: consumer,
		logger:   params.Logger,
	}

	return logReceiver, nil
}

// logsReceiver implements the receiver.Logs interface for collecting PostgreSQL logs.
type logsReceiver struct {
	settings receiver.Settings
	config   *Config
	consumer consumer.Logs
	logger   *zap.Logger
}

func (lr *logsReceiver) Start(ctx context.Context, host component.Host) error {
	lr.logger.Info("Starting New Relic PostgreSQL logs receiver")
	// TODO: Implement actual log collection logic here
	return nil
}

func (lr *logsReceiver) Shutdown(ctx context.Context) error {
	lr.logger.Info("Shutting down New Relic PostgreSQL logs receiver")
	// TODO: Implement cleanup logic here
	return nil
}

func (lr *logsReceiver) ConsumeLogFunc(ctx context.Context) func(plog.Logs) error {
	return func(ld plog.Logs) error {
		return lr.consumer.ConsumeLogs(ctx, ld)
	}
}
