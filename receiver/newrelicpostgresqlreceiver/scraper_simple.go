// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver"

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
)

type postgreSQLScraper struct {
	logger        *zap.Logger
	config        *Config
	clientFactory postgreSQLClientFactory
	mb            *metadata.MetricsBuilder
	lb            *metadata.LogsBuilder
	excludes      map[string]struct{}
	cache         *lru.Cache[string, float64]
	queryPlanCache       *expirable.LRU[string, string]
	newestQueryTimestamp float64
	nrQPC                *NewRelicQueryPerformanceCollector
}

func newPostgreSQLScraper(
	settings receiver.Settings,
	config *Config,
) (*postgreSQLScraper, error) {
	clientFactory := newClientFactory(config)

	excludes := make(map[string]struct{}, len(config.ExcludeDatabases))
	for _, db := range config.ExcludeDatabases {
		excludes[db] = struct{}{}
	}

	cache, err := lru.New[string, float64](512)
	if err != nil {
		return nil, err
	}

	queryPlanCache := expirable.NewLRU[string, string](
		config.NewRelicQueryPerformance.QueryPlanCacheSize,
		nil,
		config.NewRelicQueryPerformance.QueryPlanCacheTTL,
	)

	mb := metadata.NewMetricsBuilder(config.MetricsBuilderConfig, settings)
	lb := metadata.NewLogsBuilder(settings)

	// Create New Relic Query Performance Collector
	nrQPC := NewNewRelicQueryPerformanceCollector(nil, mb, lb, &config.NewRelicQueryPerformance, settings.Logger)

	return &postgreSQLScraper{
		logger:               settings.Logger,
		config:               config,
		clientFactory:        clientFactory,
		mb:                   mb,
		lb:                   lb,
		excludes:             excludes,
		cache:                cache,
		queryPlanCache:       queryPlanCache,
		newestQueryTimestamp: 0,
		nrQPC:                nrQPC,
	}, nil
}

func (p *postgreSQLScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	var errs scrapererror.ScrapeErrors

	databases := p.config.Databases
	if len(databases) == 0 {
		// If no databases specified, we'll connect to the default database to get the list
		dbClient, err := p.clientFactory.getClient(ctx, "postgres")
		if err != nil {
			errs.AddPartial(1, err)
			return p.mb.Emit(), errs.Combine()
		}
		defer func() {
			if closeErr := dbClient.Close(); closeErr != nil {
				p.logger.Warn("Failed to close database client", zap.Error(closeErr))
			}
		}()

		databases, err = dbClient.listDatabases(ctx)
		if err != nil {
			errs.AddPartial(1, err)
			return p.mb.Emit(), errs.Combine()
		}
	}

	// Filter excluded databases
	var filteredDatabases []string
	for _, db := range databases {
		if _, excluded := p.excludes[db]; !excluded {
			filteredDatabases = append(filteredDatabases, db)
		}
	}

	// Collect basic metrics for each database
	now := pcommon.NewTimestampFromTime(time.Now())
	for _, db := range filteredDatabases {
		p.collectBasicMetrics(ctx, db, now, &errs)
	}

	// Run New Relic Query Performance Collection if enabled
	if p.config.NewRelicQueryPerformance.Enabled && p.nrQPC != nil {
		// Connect to the first available database to collect performance metrics
		if len(filteredDatabases) > 0 {
			dbClient, err := p.clientFactory.getClient(ctx, filteredDatabases[0])
			if err != nil {
				p.logger.Error("Failed to connect for New Relic query performance collection", zap.Error(err))
				errs.AddPartial(1, err)
			} else {
				// Update the client in the New Relic collector
				p.nrQPC.client = dbClient
				
				// Collect New Relic performance metrics
				if err := p.nrQPC.CollectAll(ctx, filteredDatabases); err != nil {
					p.logger.Error("Failed to collect New Relic performance metrics", zap.Error(err))
					errs.AddPartial(1, err)
				}
				
				if closeErr := dbClient.Close(); closeErr != nil {
					p.logger.Warn("Failed to close database client", zap.Error(closeErr))
				}
			}
		}
	}

	return p.mb.Emit(), errs.Combine()
}

func (p *postgreSQLScraper) collectBasicMetrics(ctx context.Context, database string, now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	dbClient, err := p.clientFactory.getClient(ctx, database)
	if err != nil {
		errs.AddPartial(1, err)
		return
	}
	defer func() {
		if closeErr := dbClient.Close(); closeErr != nil {
			p.logger.Warn("Failed to close database client", zap.Error(closeErr))
		}
	}()

	// Collect basic database stats
	stats, err := dbClient.getDatabaseStats(ctx, []string{database})
	if err != nil {
		errs.AddPartial(1, err)
		return
	}

	if dbStats, exists := stats[databaseName(database)]; exists {
		// Record metrics that are available in our metadata
		p.mb.RecordPostgresqlCommitsDataPoint(now, dbStats.transactionCommitted, database)
		p.mb.RecordPostgresqlRollbacksDataPoint(now, dbStats.transactionRollback, database)
	}

	// Collect connection count
	backends, err := dbClient.getBackends(ctx, []string{database})
	if err != nil {
		errs.AddPartial(1, err)
		return
	}

	if connectionCount, exists := backends[databaseName(database)]; exists {
		p.mb.RecordPostgresqlConnectionCountDataPoint(now, connectionCount, database)
	}

	// Collect max connections
	maxConnections, err := dbClient.getMaxConnections(ctx)
	if err != nil {
		errs.AddPartial(1, err)
		return
	}
	p.mb.RecordPostgresqlConnectionMaxDataPoint(now, maxConnections)

	// Collect database size
	sizes, err := dbClient.getDatabaseSize(ctx, []string{database})
	if err != nil {
		errs.AddPartial(1, err)
		return
	}

	if size, exists := sizes[databaseName(database)]; exists {
		// Note: We don't have a database size metric in our metadata, so we'll skip this for now
		_ = size
	}
}

func (p *postgreSQLScraper) scrapeLogs(ctx context.Context) (plog.Logs, error) {
	// If New Relic performance collection is enabled, return the logs
	if p.config.NewRelicQueryPerformance.Enabled && p.nrQPC != nil {
		return p.lb.Emit(), nil
	}
	return plog.NewLogs(), nil
}

func (p *postgreSQLScraper) shutdown(_ context.Context) error {
	return nil
}
