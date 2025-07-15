// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver"

import (
	"context"
	"fmt"
	"strings"
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

	// Collect server-wide metrics if we have databases available
	if len(filteredDatabases) > 0 {
		serverClient, err := p.clientFactory.getClient(filteredDatabases[0])
		if err != nil {
			errs.add(err)
		} else {
			defer func() {
				if closeErr := serverClient.Close(); closeErr != nil {
					p.logger.Warn("Failed to close server client", zap.Error(closeErr))
				}
			}()
			
			// Collect BGWriter stats
			p.collectBGWriterStats(ctx, now, serverClient, &errs)
			
			// Collect WAL age
			p.collectWalAge(ctx, now, serverClient, &errs)
			
			// Collect replication stats
			p.collectReplicationStats(ctx, now, serverClient, &errs)
			
			// Collect database locks
			p.collectDatabaseLocks(ctx, now, serverClient, &errs)
		}
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
		// Record basic transaction metrics
		p.mb.RecordPostgresqlCommitsDataPoint(now, dbStats.transactionCommitted, database)
		p.mb.RecordPostgresqlRollbacksDataPoint(now, dbStats.transactionRollback, database)
		
		// Record additional database stats
		p.mb.RecordPostgresqlBlksHitDataPoint(now, dbStats.blksHit, database)
		p.mb.RecordPostgresqlBlksReadDataPoint(now, dbStats.blksRead, database)
		p.mb.RecordPostgresqlTupReturnedDataPoint(now, dbStats.tupReturned, database)
		p.mb.RecordPostgresqlTupFetchedDataPoint(now, dbStats.tupFetched, database)
		p.mb.RecordPostgresqlTupInsertedDataPoint(now, dbStats.tupInserted, database)
		p.mb.RecordPostgresqlTupUpdatedDataPoint(now, dbStats.tupUpdated, database)
		p.mb.RecordPostgresqlTupDeletedDataPoint(now, dbStats.tupDeleted, database)
		p.mb.RecordPostgresqlDeadlocksDataPoint(now, dbStats.deadlocks, database)
		p.mb.RecordPostgresqlTempFilesDataPoint(now, dbStats.tempFiles, database)
	}

	// Collect connection count
	backends, err := dbClient.getBackends(ctx, []string{database})
	if err != nil {
		errs.add(err)
		return
	}

	if connectionCount, exists := backends[databaseName(database)]; exists {
		p.mb.RecordPostgresqlConnectionCountDataPoint(now, connectionCount, database)
		p.mb.RecordPostgresqlBackendsDataPoint(now, connectionCount)
	}

	// Collect database size
	dbSizes, err := dbClient.getDatabaseSize(ctx, []string{database})
	if err == nil {
		if dbSize, exists := dbSizes[databaseName(database)]; exists {
			p.mb.RecordPostgresqlDbSizeDataPoint(now, dbSize, database)
		}
	} else {
		errs.add(err)
	}

	// Collect table metrics for this database
	numTables := p.collectTables(ctx, now, dbClient, database, errs)
	p.mb.RecordPostgresqlTableCountDataPoint(now, numTables, database)
	
	// Collect index metrics for this database
	p.collectIndexes(ctx, now, dbClient, database, errs)

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

// collectBGWriterStats collects background writer statistics
func (p *postgreSQLScraper) collectBGWriterStats(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	errs *errsMux,
) {
	bgStats, err := client.getBGWriterStats(ctx)
	if err != nil {
		errs.add(err)
		return
	}

	p.mb.RecordPostgresqlBgwriterBuffersAllocatedDataPoint(now, bgStats.buffersAllocated)

	p.mb.RecordPostgresqlBgwriterBuffersWritesDataPoint(now, bgStats.bgWrites, metadata.AttributeBgBufferSourceBgwriter)
	if bgStats.bufferBackendWrites >= 0 {
		p.mb.RecordPostgresqlBgwriterBuffersWritesDataPoint(now, bgStats.bufferBackendWrites, metadata.AttributeBgBufferSourceBackend)
	}
	p.mb.RecordPostgresqlBgwriterBuffersWritesDataPoint(now, bgStats.bufferCheckpoints, metadata.AttributeBgBufferSourceCheckpoints)
	if bgStats.bufferFsyncWrites >= 0 {
		p.mb.RecordPostgresqlBgwriterBuffersWritesDataPoint(now, bgStats.bufferFsyncWrites, metadata.AttributeBgBufferSourceBackendFsync)
	}

	p.mb.RecordPostgresqlBgwriterCheckpointCountDataPoint(now, bgStats.checkpointsReq, metadata.AttributeBgCheckpointTypeRequested)
	p.mb.RecordPostgresqlBgwriterCheckpointCountDataPoint(now, bgStats.checkpointsScheduled, metadata.AttributeBgCheckpointTypeScheduled)

	p.mb.RecordPostgresqlBgwriterDurationDataPoint(now, bgStats.checkpointSyncTime, metadata.AttributeBgDurationTypeSync)
	p.mb.RecordPostgresqlBgwriterDurationDataPoint(now, bgStats.checkpointWriteTime, metadata.AttributeBgDurationTypeWrite)

	p.mb.RecordPostgresqlBgwriterMaxwrittenDataPoint(now, bgStats.maxWritten)
}

// collectWalAge collects WAL age statistics
func (p *postgreSQLScraper) collectWalAge(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	errs *errsMux,
) {
	walAge, err := client.getLatestWalAgeSeconds(ctx)
	if err != nil {
		// Check if this is the "no last archive" error, which is common when archiving is not configured
		if strings.Contains(err.Error(), "no last archive found") {
			p.logger.Debug("WAL archiving not configured, skipping WAL age metric", zap.Error(err))
			return
		}
		errs.add(err)
		return
	}

	p.mb.RecordPostgresqlWalAgeDataPoint(now, walAge)
}

// collectReplicationStats collects replication lag statistics
func (p *postgreSQLScraper) collectReplicationStats(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	errs *errsMux,
) {
	replicationStats, err := client.getReplicationStats(ctx)
	if err != nil {
		errs.add(err)
		return
	}

	for _, stat := range replicationStats {
		clientAddr := stat.clientAddr
		if clientAddr == "" {
			clientAddr = "unknown"
		}
		
		p.mb.RecordPostgresqlReplicationDataDelayDataPoint(now, stat.pendingBytes, clientAddr)
		p.mb.RecordPostgresqlWalDelayDataPoint(now, stat.replayLag, clientAddr)
	}
}

// collectDatabaseLocks collects database lock statistics
func (p *postgreSQLScraper) collectDatabaseLocks(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	errs *errsMux,
) {
	locks, err := client.getDatabaseLocks(ctx)
	if err != nil {
		errs.add(err)
		return
	}

	// Database locks don't include database names in this receiver
	// They are relation-level locks, so we count them differently
	totalLocks := int64(0)
	for _, lock := range locks {
		totalLocks += lock.locks
	}

	// Record total locks (since we don't have per-database info)
	if len(locks) > 0 {
		p.mb.RecordPostgresqlDatabaseLocksDataPoint(now, totalLocks, "all")
	}
}

// collectTables collects table-level metrics for a database
func (p *postgreSQLScraper) collectTables(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	database string,
	errs *errsMux,
) int64 {
	tableMetrics, err := client.getDatabaseTableMetrics(ctx, database)
	if err != nil {
		errs.add(err)
		return 0
	}

	tableIOStats, err := client.getBlocksReadByTable(ctx, database)
	if err != nil {
		errs.add(err)
		return 0
	}

	tableCount := int64(len(tableMetrics))

	for tableKey, metrics := range tableMetrics {
		parts := strings.Split(string(tableKey), "|")
		if len(parts) != 2 {
			continue
		}
		dbName := parts[0]
		tableFullName := parts[1]

		// Extract schema and table name
		schemaParts := strings.Split(tableFullName, ".")
		var schemaName, tableName string
		if len(schemaParts) == 2 {
			schemaName = schemaParts[0]
			tableName = schemaParts[1]
		} else {
			schemaName = "public"
			tableName = tableFullName
		}

		// Record table size
		p.mb.RecordPostgresqlTableSizeDataPoint(now, metrics.size, dbName, schemaName, tableName)

		// Record table vacuum count
		p.mb.RecordPostgresqlTableVacuumCountDataPoint(now, metrics.vacuumCount, dbName, schemaName, tableName)

		// Record sequential scans
		p.mb.RecordPostgresqlSequentialScansDataPoint(now, metrics.seqScans, dbName, schemaName, tableName)

		// Record table operations
		p.mb.RecordPostgresqlOperationsDataPoint(now, metrics.inserts, dbName, tableName, metadata.AttributeOperationIns)
		p.mb.RecordPostgresqlOperationsDataPoint(now, metrics.upd, dbName, tableName, metadata.AttributeOperationUpd)
		p.mb.RecordPostgresqlOperationsDataPoint(now, metrics.del, dbName, tableName, metadata.AttributeOperationDel)
		p.mb.RecordPostgresqlOperationsDataPoint(now, metrics.hotUpd, dbName, tableName, metadata.AttributeOperationHotUpd)

		// Record row states
		p.mb.RecordPostgresqlRowsDataPoint(now, metrics.live, dbName, tableName, metadata.AttributeStateLive)
		p.mb.RecordPostgresqlRowsDataPoint(now, metrics.dead, dbName, tableName, metadata.AttributeStateDead)

		// Record blocks read/hit if available
		if ioStats, exists := tableIOStats[tableKey]; exists {
			p.mb.RecordPostgresqlBlocksReadDataPoint(now, ioStats.heapRead, dbName, tableName, metadata.AttributeSourceHeapRead)
			p.mb.RecordPostgresqlBlocksHitDataPoint(now, ioStats.heapHit, dbName, tableName, metadata.AttributeSourceHeapHit)
			p.mb.RecordPostgresqlBlocksReadDataPoint(now, ioStats.idxRead, dbName, tableName, metadata.AttributeSourceIdxRead)
			p.mb.RecordPostgresqlBlocksHitDataPoint(now, ioStats.idxHit, dbName, tableName, metadata.AttributeSourceIdxHit)
		}
	}

	return tableCount
}

// collectIndexes collects index-level metrics for a database
func (p *postgreSQLScraper) collectIndexes(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	database string,
	errs *errsMux,
) {
	indexStats, err := client.getIndexStats(ctx, database)
	if err != nil {
		errs.add(err)
		return
	}

	for indexKey, stats := range indexStats {
		parts := strings.Split(string(indexKey), "|")
		if len(parts) != 3 {
			continue
		}
		dbName := parts[0]
		tableFullName := parts[1]
		indexName := parts[2]

		// Extract schema and table name
		schemaParts := strings.Split(tableFullName, ".")
		var schemaName, tableName string
		if len(schemaParts) == 2 {
			schemaName = schemaParts[0]
			tableName = schemaParts[1]
		} else {
			schemaName = "public"
			tableName = tableFullName
		}

		// Record index scans
		p.mb.RecordPostgresqlIndexScansDataPoint(now, stats.scans, dbName, schemaName, tableName, indexName)

		// Record index size
		p.mb.RecordPostgresqlIndexSizeDataPoint(now, stats.size, dbName, schemaName, tableName, indexName)
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
