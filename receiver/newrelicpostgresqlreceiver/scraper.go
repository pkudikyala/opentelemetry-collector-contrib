// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver"

import (
	"context"
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
)

type postgreSQLScraper struct {
	logger               *zap.Logger
	config               *Config
	clientFactory        postgreSQLClientFactory
	mb                   *metadata.MetricsBuilder
	lb                   *metadata.LogsBuilder
	excludes             map[string]struct{}
	cache                *lru.Cache[string, float64]
	queryPlanCache       *expirable.LRU[string, string]
	newestQueryTimestamp float64
	nrQPC                *NewRelicQueryPerformanceCollector
}

type errsMux struct {
	sync.RWMutex
	errs []error
}

func (e *errsMux) add(err error) {
	e.Lock()
	defer e.Unlock()
	e.errs = append(e.errs, err)
}

func (e *errsMux) combine() error {
	e.RLock()
	defer e.RUnlock()
	if len(e.errs) == 0 {
		return nil
	}
	if len(e.errs) == 1 {
		return e.errs[0]
	}
	// Combine multiple errors into a single error message
	var msg string
	for i, err := range e.errs {
		if i > 0 {
			msg += "; "
		}
		msg += err.Error()
	}
	return fmt.Errorf("multiple errors: %s", msg)
}

func newPostgreSQLScraper(
	settings receiver.Settings,
	config *Config,
) (*postgreSQLScraper, error) {
	clientFactory := newDefaultClientFactory(config)

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
	nrQPC := NewNewRelicQueryPerformanceCollector(settings.Logger, mb, lb, &config.NewRelicQueryPerformance, nil)

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
	var errs errsMux

	databases := p.config.Databases
	if len(databases) == 0 {
		// If no databases specified, we'll connect to the default database to get the list
		dbClient, err := p.clientFactory.getClient("postgres")
		if err != nil {
			errs.add(err)
			return p.mb.Emit(), nil
		}
		defer func() {
			if closeErr := dbClient.Close(); closeErr != nil {
				p.logger.Warn("Failed to close database client", zap.Error(closeErr))
			}
		}()

		databases, err = dbClient.listDatabases(ctx)
		if err != nil {
			errs.add(err)
			return p.mb.Emit(), nil
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
	if p.config.NewRelicQueryPerformance.Enabled && p.nrQPC != nil && len(filteredDatabases) > 0 {
		// Connect to the first available database to collect performance metrics
		dbClient, err := p.clientFactory.getClient(filteredDatabases[0])
		if err != nil {
			p.logger.Error("Failed to connect for New Relic query performance collection", zap.Error(err))
			errs.add(err)
		} else {
			// Update the client in the New Relic collector
			p.nrQPC.client = dbClient
			
			// Collect New Relic performance metrics
			if err := p.nrQPC.CollectQueryPerformanceMetrics(ctx, filteredDatabases); err != nil {
				p.logger.Error("Failed to collect New Relic performance metrics", zap.Error(err))
				errs.add(err)
			}
			
			if closeErr := dbClient.Close(); closeErr != nil {
				p.logger.Warn("Failed to close database client", zap.Error(closeErr))
			}
		}
	}

	return p.mb.Emit(), errs.combine()
}

func (p *postgreSQLScraper) collectBasicMetrics(ctx context.Context, database string, now pcommon.Timestamp, errs *errsMux) {
	dbClient, err := p.clientFactory.getClient(database)
	if err != nil {
		errs.add(err)
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
		errs.add(err)
		return
	}

	if dbStats, exists := stats[databaseName(database)]; exists {
		// Record metrics that are available in our metadata - note the database name is required
		p.mb.RecordPostgresqlCommitsDataPoint(now, dbStats.transactionCommitted, database)
		p.mb.RecordPostgresqlRollbacksDataPoint(now, dbStats.transactionRollback, database)
	}

	// Collect connection count
	backends, err := dbClient.getBackends(ctx, []string{database})
	if err != nil {
		errs.add(err)
		return
	}

	if connectionCount, exists := backends[databaseName(database)]; exists {
		p.mb.RecordPostgresqlConnectionCountDataPoint(now, connectionCount, database)
	}

	// Collect max connections (only needs to be done once, not per database)
	maxConnections, err := dbClient.getMaxConnections(ctx)
	if err == nil {
		p.mb.RecordPostgresqlConnectionMaxDataPoint(now, maxConnections)
	} else {
		errs.add(err)
	}

	// Collect database count (server-wide metric)
	databases, err := dbClient.listDatabases(ctx)
	if err == nil {
		p.mb.RecordPostgresqlDatabaseCountDataPoint(now, int64(len(databases)))
	} else {
		errs.add(err)
	}
}

func (p *postgreSQLScraper) scrapeLogs(ctx context.Context) (plog.Logs, error) {
	// Return logs generated by New Relic performance collection
	return p.lb.Emit(), nil
}

func (p *postgreSQLScraper) shutdown(_ context.Context) error {
	// Close the client factory to clean up connections
	if p.clientFactory != nil {
		if err := p.clientFactory.close(); err != nil {
			p.logger.Warn("Failed to close client factory", zap.Error(err))
		}
	}
	
	// The expirable LRU cache doesn't have an explicit close method,
	// but we can clear it to help with cleanup
	if p.queryPlanCache != nil {
		p.queryPlanCache.Purge()
	}
	
	return nil
}
