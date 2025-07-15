// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
)

// NewRelicQueryPerformanceCollector handles collection of New Relic specific query performance metrics
type NewRelicQueryPerformanceCollector struct {
	logger           *zap.Logger
	mb               *metadata.MetricsBuilder
	lb               *metadata.LogsBuilder
	config           *NewRelicQueryPerformanceConfig
	client           client
	slowQueryCache   map[string]*SlowQueryInfo
	executionPlans   map[string]*ExecutionPlan
	lastCollectedAt  time.Time
}

// SlowQueryInfo represents information about a slow query
type SlowQueryInfo struct {
	QueryID           string    `json:"query_id"`
	QueryText         string    `json:"query_text"`
	DatabaseName      string    `json:"database_name"`
	SchemaName        string    `json:"schema_name"`
	ExecutionCount    int64     `json:"execution_count"`
	AvgElapsedTimeMs  float64   `json:"avg_elapsed_time_ms"`
	AvgDiskReads      float64   `json:"avg_disk_reads"`
	AvgDiskWrites     float64   `json:"avg_disk_writes"`
	StatementType     string    `json:"statement_type"`
	CollectionTime    time.Time `json:"collection_time"`
	IndividualQuery   string    `json:"individual_query,omitempty"`
}

// WaitEventInfo represents wait event metrics
type WaitEventInfo struct {
	WaitEventName   string  `json:"wait_event_name"`
	WaitCategory    string  `json:"wait_category"`
	TotalWaitTimeMs float64 `json:"total_wait_time_ms"`
	QueryID         string  `json:"query_id"`
	QueryText       string  `json:"query_text"`
	DatabaseName    string  `json:"database_name"`
}

// BlockingSessionInfo represents blocking session information
type BlockingSessionInfo struct {
	BlockedPid       int64  `json:"blocked_pid"`
	BlockingPid      int64  `json:"blocking_pid"`
	BlockedQuery     string `json:"blocked_query"`
	BlockingQuery    string `json:"blocking_query"`
	DatabaseName     string `json:"database_name"`
	WaitEventType    string `json:"wait_event_type"`
	WaitEvent        string `json:"wait_event"`
	BlockingDuration string `json:"blocking_duration"`
}

// ExecutionPlan represents query execution plan information
type ExecutionPlan struct {
	QueryID      string                 `json:"query_id"`
	DatabaseName string                 `json:"database_name"`
	PlanID       string                 `json:"plan_id"`
	Plan         map[string]interface{} `json:"plan"`
	CreatedAt    time.Time              `json:"created_at"`
}

// IndividualQueryInfo represents individual query metrics
type IndividualQueryInfo struct {
	QueryID         string  `json:"query_id"`
	QueryText       string  `json:"query_text"`
	DatabaseName    string  `json:"database_name"`
	PlanID          string  `json:"plan_id"`
	CPUTimeMs       float64 `json:"cpu_time_ms"`
	ExecTimeMs      float64 `json:"exec_time_ms"`
	RealQueryText   string  `json:"real_query_text"`
}

// NewRelicQueryPerformanceCollector creates a new instance of the collector
func NewNewRelicQueryPerformanceCollector(logger *zap.Logger, mb *metadata.MetricsBuilder, lb *metadata.LogsBuilder, config *NewRelicQueryPerformanceConfig, client client) *NewRelicQueryPerformanceCollector {
	return &NewRelicQueryPerformanceCollector{
		logger:         logger,
		mb:             mb,
		lb:             lb,
		config:         config,
		client:         client,
		slowQueryCache: make(map[string]*SlowQueryInfo),
		executionPlans: make(map[string]*ExecutionPlan),
		lastCollectedAt: time.Now(),
	}
}

// CollectQueryPerformanceMetrics collects New Relic style query performance metrics
func (nrqpc *NewRelicQueryPerformanceCollector) CollectQueryPerformanceMetrics(ctx context.Context, databases []string) error {
	if !nrqpc.config.Enabled {
		return nil
	}

	nrqpc.logger.Debug("Starting New Relic query performance metrics collection")

	// Collect slow queries
	if nrqpc.config.QueryMonitoringEnabled {
		if err := nrqpc.collectSlowQueries(ctx, databases); err != nil {
			nrqpc.logger.Error("Failed to collect slow queries", zap.Error(err))
		}
	}

	// Collect wait events
	if nrqpc.config.WaitEventMonitoringEnabled {
		if err := nrqpc.collectWaitEvents(ctx, databases); err != nil {
			nrqpc.logger.Error("Failed to collect wait events", zap.Error(err))
		}
	}

	// Collect blocking sessions
	if nrqpc.config.BlockingSessionsEnabled {
		if err := nrqpc.collectBlockingSessions(ctx, databases); err != nil {
			nrqpc.logger.Error("Failed to collect blocking sessions", zap.Error(err))
		}
	}

	// Collect individual queries
	if nrqpc.config.IndividualQueryEnabled {
		if err := nrqpc.collectIndividualQueries(ctx, databases); err != nil {
			nrqpc.logger.Error("Failed to collect individual queries", zap.Error(err))
		}
	}

	// Collect execution plans
	if nrqpc.config.ExecutionPlanEnabled {
		if err := nrqpc.collectExecutionPlans(ctx, databases); err != nil {
			nrqpc.logger.Error("Failed to collect execution plans", zap.Error(err))
		}
	}

	return nil
}

// collectSlowQueries collects slow running queries similar to New Relic's implementation
func (nrqpc *NewRelicQueryPerformanceCollector) collectSlowQueries(ctx context.Context, databases []string) error {
	// Query adapted from New Relic's SlowQueriesForV13AndAbove
	query := fmt.Sprintf(`
		SELECT 
			pss.queryid AS query_id,
			LEFT(pss.query, 4095) AS query_text,
			pd.datname AS database_name,
			CASE 
				-- If query contains schema-qualified names (schema.table), indicate mixed schemas
				WHEN pss.query ~* '\w+\.\w+' THEN 'schema_qualified'
				-- For unqualified table names, assume public schema (PostgreSQL default)
				ELSE 'public'
			END AS schema_name,
			pss.calls AS execution_count,
			ROUND((pss.total_exec_time / pss.calls)::numeric, 3) AS avg_elapsed_time_ms,
			pss.shared_blks_read / pss.calls AS avg_disk_reads,
			pss.shared_blks_written / pss.calls AS avg_disk_writes,
			CASE
				WHEN pss.query ILIKE 'SELECT%%' THEN 'SELECT'
				WHEN pss.query ILIKE 'INSERT%%' THEN 'INSERT'
				WHEN pss.query ILIKE 'UPDATE%%' THEN 'UPDATE'
				WHEN pss.query ILIKE 'DELETE%%' THEN 'DELETE'
				ELSE 'OTHER'
			END AS statement_type,
			to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS collection_timestamp
		FROM 
			pg_stat_statements pss
		JOIN 
			pg_database pd ON pss.dbid = pd.oid
		WHERE 
			pd.datname IN (%s)
			AND pss.calls >= %d
			AND (pss.total_exec_time / pss.calls) >= %d
		ORDER BY 
			avg_elapsed_time_ms DESC
		LIMIT %d`,
		nrqpc.formatDatabaseList(databases),
		1, // minimum calls (reduced from 5)
		nrqpc.config.SlowQueryThresholdMs,
		nrqpc.config.MaxQueriesPerCollection,
	)

	rows, err := nrqpc.client.Query(ctx, query)
	if err != nil {
		nrqpc.logger.Error("Failed to execute slow queries query", zap.Error(err), zap.String("query", query))
		return fmt.Errorf("failed to execute slow queries query: %w", err)
	}
	defer rows.Close()

	nrqpc.logger.Debug("Executing slow queries collection", zap.String("query", query))

	now := pcommon.NewTimestampFromTime(time.Now())
	slowQueryCount := 0

	for rows.Next() {
		var slowQuery SlowQueryInfo
		var collectionTimestamp string

		err := rows.Scan(
			&slowQuery.QueryID,
			&slowQuery.QueryText,
			&slowQuery.DatabaseName,
			&slowQuery.SchemaName,
			&slowQuery.ExecutionCount,
			&slowQuery.AvgElapsedTimeMs,
			&slowQuery.AvgDiskReads,
			&slowQuery.AvgDiskWrites,
			&slowQuery.StatementType,
			&collectionTimestamp,
		)
		if err != nil {
			nrqpc.logger.Error("Failed to scan slow query row", zap.Error(err))
			continue
		}

		slowQueryCount++
		nrqpc.logger.Debug("Found slow query", 
			zap.String("query_id", slowQuery.QueryID),
			zap.String("database_name", slowQuery.DatabaseName),
			zap.Int64("execution_count", slowQuery.ExecutionCount),
			zap.Float64("avg_elapsed_time_ms", slowQuery.AvgElapsedTimeMs),
		)

		// Cache the slow query for later use
		nrqpc.slowQueryCache[slowQuery.QueryID] = &slowQuery

		// Record metrics
		nrqpc.mb.RecordPostgresqlQueryExecutionCountDataPoint(now, slowQuery.ExecutionCount, slowQuery.DatabaseName, slowQuery.SchemaName, slowQuery.QueryID, slowQuery.QueryText, slowQuery.StatementType)
		nrqpc.mb.RecordPostgresqlQueryAvgElapsedTimeDataPoint(now, slowQuery.AvgElapsedTimeMs, slowQuery.DatabaseName, slowQuery.SchemaName, slowQuery.QueryID, slowQuery.QueryText, slowQuery.StatementType)
		nrqpc.mb.RecordPostgresqlQueryAvgDiskReadsDataPoint(now, slowQuery.AvgDiskReads, slowQuery.DatabaseName, slowQuery.SchemaName, slowQuery.QueryID, slowQuery.QueryText, slowQuery.StatementType)
		nrqpc.mb.RecordPostgresqlQueryAvgDiskWritesDataPoint(now, slowQuery.AvgDiskWrites, slowQuery.DatabaseName, slowQuery.SchemaName, slowQuery.QueryID, slowQuery.QueryText, slowQuery.StatementType)

		// Generate log entry for slow query
		nrqpc.generateSlowQueryLog(slowQuery)
	}

	nrqpc.logger.Debug("Slow query collection completed", zap.Int("slow_query_count", slowQueryCount))

	return nil
}

// collectWaitEvents collects wait event information
func (nrqpc *NewRelicQueryPerformanceCollector) collectWaitEvents(ctx context.Context, databases []string) error {
	// Query adapted from New Relic's WaitEvents query
	query := fmt.Sprintf(`
		SELECT
			sa.wait_event_type || ':' || COALESCE(sa.wait_event, 'unknown') AS wait_event_name,
			CASE
				WHEN sa.wait_event_type IN ('LWLock', 'Lock') THEN 'Locks'
				WHEN sa.wait_event_type = 'IO' THEN 'Disk IO'
				WHEN sa.wait_event_type = 'CPU' THEN 'CPU'
				ELSE 'Other'
			END AS wait_category,
			EXTRACT(EPOCH FROM (NOW() - sa.query_start)) * 1000 AS total_wait_time_ms,
			COALESCE(pss.queryid::text, 'unknown') AS query_id,
			LEFT(sa.query, 4095) AS query_text,
			pd.datname AS database_name
		FROM 
			pg_stat_activity sa
		LEFT JOIN 
			pg_stat_statements pss ON pss.query = sa.query AND pss.dbid = sa.datid
		LEFT JOIN 
			pg_database pd ON pd.oid = sa.datid
		WHERE 
			pd.datname IN (%s)
			AND sa.wait_event_type IS NOT NULL
			AND sa.state = 'active'
			AND sa.query NOT LIKE 'EXPLAIN (FORMAT JSON) %%'
		ORDER BY 
			total_wait_time_ms DESC
		LIMIT %d`,
		nrqpc.formatDatabaseList(databases),
		int(nrqpc.config.MaxQueriesPerCollection),
	)

	rows, err := nrqpc.client.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute wait events query: %w", err)
	}
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var waitEvent WaitEventInfo

		err := rows.Scan(
			&waitEvent.WaitEventName,
			&waitEvent.WaitCategory,
			&waitEvent.TotalWaitTimeMs,
			&waitEvent.QueryID,
			&waitEvent.QueryText,
			&waitEvent.DatabaseName,
		)
		if err != nil {
			nrqpc.logger.Error("Failed to scan wait event row", zap.Error(err))
			continue
		}

		// Record wait event metrics
		nrqpc.mb.RecordPostgresqlWaitEventTotalTimeDataPoint(now, waitEvent.TotalWaitTimeMs, waitEvent.DatabaseName, waitEvent.QueryID, waitEvent.QueryText, waitEvent.WaitEventName, waitEvent.WaitCategory)
	}

	return nil
}

// collectBlockingSessions collects information about blocking and blocked sessions
func (nrqpc *NewRelicQueryPerformanceCollector) collectBlockingSessions(ctx context.Context, databases []string) error {
	// Query adapted from New Relic's BlockingQueriesForV14AndAbove
	query := fmt.Sprintf(`
		SELECT
			blocked_activity.pid AS blocked_pid,
			blocking_activity.pid AS blocking_pid,
			LEFT(blocked_activity.query, 4095) AS blocked_query,
			LEFT(blocking_activity.query, 4095) AS blocking_query,
			blocked_db.datname AS database_name,
			blocked_activity.wait_event_type,
			blocked_activity.wait_event,
			EXTRACT(EPOCH FROM (NOW() - blocked_activity.query_start)) AS blocking_duration
		FROM 
			pg_catalog.pg_locks blocked_locks
		JOIN 
			pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
		JOIN 
			pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
		JOIN 
			pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
		JOIN 
			pg_database blocked_db ON blocked_db.oid = blocked_activity.datid
		WHERE 
			NOT blocked_locks.granted
			AND blocking_locks.granted
			AND blocked_locks.pid != blocking_locks.pid
			AND blocked_db.datname IN (%s)
			AND blocked_activity.query NOT LIKE 'EXPLAIN (FORMAT JSON) %%'
			AND blocking_activity.query NOT LIKE 'EXPLAIN (FORMAT JSON) %%'
		ORDER BY 
			blocked_activity.query_start ASC
		LIMIT %d`,
		nrqpc.formatDatabaseList(databases),
		int(nrqpc.config.MaxQueriesPerCollection),
	)

	rows, err := nrqpc.client.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute blocking sessions query: %w", err)
	}
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var blockingSession BlockingSessionInfo
		var blockingDurationFloat float64

		err := rows.Scan(
			&blockingSession.BlockedPid,
			&blockingSession.BlockingPid,
			&blockingSession.BlockedQuery,
			&blockingSession.BlockingQuery,
			&blockingSession.DatabaseName,
			&blockingSession.WaitEventType,
			&blockingSession.WaitEvent,
			&blockingDurationFloat,
		)
		if err != nil {
			nrqpc.logger.Error("Failed to scan blocking session row", zap.Error(err))
			continue
		}

		blockingSession.BlockingDuration = fmt.Sprintf("%.2fs", blockingDurationFloat)

		// Record blocking session metrics
		nrqpc.mb.RecordPostgresqlBlockingSessionPidDataPoint(now, blockingSession.BlockingPid, blockingSession.DatabaseName, blockingSession.BlockingQuery)
		nrqpc.mb.RecordPostgresqlBlockedSessionPidDataPoint(now, blockingSession.BlockedPid, blockingSession.DatabaseName, blockingSession.BlockedQuery)
		
		// Record new blocking session metrics
		nrqpc.mb.RecordPostgresqlBlockingSessionDurationDataPoint(now, blockingDurationFloat, blockingSession.DatabaseName, blockingSession.BlockedQuery, blockingSession.BlockingQuery)
		nrqpc.mb.RecordPostgresqlBlockingSessionWaitEventTypeDataPoint(now, 1, blockingSession.DatabaseName, blockingSession.BlockedQuery, blockingSession.BlockingQuery, blockingSession.WaitEventType)
		nrqpc.mb.RecordPostgresqlBlockingSessionWaitEventDataPoint(now, 1, blockingSession.DatabaseName, blockingSession.BlockedQuery, blockingSession.BlockingQuery, blockingSession.WaitEvent)
	}

	return nil
}

// collectIndividualQueries collects individual query metrics
func (nrqpc *NewRelicQueryPerformanceCollector) collectIndividualQueries(ctx context.Context, databases []string) error {
	// Get individual queries from pg_stat_activity (active queries)
	query := fmt.Sprintf(`
		SELECT 
			COALESCE(pss.queryid::text, md5(sa.query)) AS query_id,
			LEFT(sa.query, 4095) AS query_text,
			pd.datname AS database_name,
			md5(sa.query || NOW()::text) AS plan_id,
			0 AS cpu_time_ms,
			EXTRACT(EPOCH FROM (NOW() - sa.query_start)) * 1000 AS exec_time_ms,
			sa.query AS real_query_text
		FROM 
			pg_stat_activity sa
		LEFT JOIN 
			pg_stat_statements pss ON pss.query = sa.query AND pss.dbid = sa.datid
		LEFT JOIN 
			pg_database pd ON pd.oid = sa.datid
		WHERE 
			pd.datname IN (%s)
			AND sa.query IS NOT NULL
			AND sa.query != ''
			AND sa.state = 'active'
			AND sa.query NOT LIKE 'EXPLAIN (FORMAT JSON) %%'
		LIMIT %d`,
		nrqpc.formatDatabaseList(databases),
		int(nrqpc.config.MaxQueriesPerCollection),
	)

	rows, err := nrqpc.client.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute individual queries query: %w", err)
	}
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var individualQuery IndividualQueryInfo

		err := rows.Scan(
			&individualQuery.QueryID,
			&individualQuery.QueryText,
			&individualQuery.DatabaseName,
			&individualQuery.PlanID,
			&individualQuery.CPUTimeMs,
			&individualQuery.ExecTimeMs,
			&individualQuery.RealQueryText,
		)
		if err != nil {
			nrqpc.logger.Error("Failed to scan individual query row", zap.Error(err))
			continue
		}

		// Record individual query CPU time
		nrqpc.mb.RecordPostgresqlQueryCPUTimeDataPoint(now, individualQuery.CPUTimeMs, individualQuery.DatabaseName, "", individualQuery.QueryID, individualQuery.QueryText)
	}

	return nil
}

// collectExecutionPlans collects detailed execution plans for slow queries
func (nrqpc *NewRelicQueryPerformanceCollector) collectExecutionPlans(ctx context.Context, databases []string) error {
	now := pcommon.NewTimestampFromTime(time.Now())
	
	// For each slow query in cache, get detailed execution plan
	for queryID, slowQuery := range nrqpc.slowQueryCache {
		if _, exists := nrqpc.executionPlans[queryID]; exists {
			continue // Already have plan for this query
		}

		// Skip queries that can't be explained
		if nrqpc.shouldSkipExecutionPlan(slowQuery.QueryText) {
			nrqpc.logger.Debug("Skipping execution plan for query", 
				zap.String("query_id", queryID),
				zap.String("reason", "query type not suitable for EXPLAIN"),
			)
			continue
		}

		// Clean the query text for EXPLAIN (remove parameters)
		cleanedQuery := nrqpc.cleanQueryForExplain(slowQuery.QueryText)
		
		// Get execution plan with detailed information but without actual execution
		planQuery := fmt.Sprintf(`
			EXPLAIN (
				FORMAT JSON, 
				ANALYZE false, 
				VERBOSE true, 
				COSTS true, 
				SETTINGS true, 
				BUFFERS false
			) %s`, cleanedQuery)
		
		nrqpc.logger.Debug("Getting execution plan for slow query", 
			zap.String("query_id", queryID),
			zap.String("database", slowQuery.DatabaseName),
		)

		rows, err := nrqpc.client.Query(ctx, planQuery)
		if err != nil {
			nrqpc.logger.Debug("Failed to get execution plan", 
				zap.String("query_id", queryID), 
				zap.String("database", slowQuery.DatabaseName),
				zap.Error(err))
			continue
		}

		var planJSON string
		if rows.Next() {
			if err := rows.Scan(&planJSON); err != nil {
				nrqpc.logger.Error("Failed to scan execution plan", zap.Error(err))
				rows.Close()
				continue
			}
		}
		rows.Close()

		// Parse execution plan
		var plan []map[string]interface{}
		if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
			nrqpc.logger.Error("Failed to parse execution plan JSON", zap.Error(err))
			continue
		}

		if len(plan) > 0 {
			executionPlan := &ExecutionPlan{
				QueryID:      queryID,
				DatabaseName: slowQuery.DatabaseName,
				PlanID:       fmt.Sprintf("plan_%s_%d", queryID, time.Now().Unix()),
				Plan:         plan[0],
				CreatedAt:    time.Now(),
			}

			nrqpc.executionPlans[queryID] = executionPlan
			nrqpc.recordDetailedExecutionPlanMetrics(executionPlan, now)
			
			nrqpc.logger.Debug("Successfully recorded execution plan metrics", 
				zap.String("query_id", queryID),
				zap.String("plan_id", executionPlan.PlanID),
			)
		}
	}

	return nil
}

// recordDetailedExecutionPlanMetrics records comprehensive execution plan metrics
func (nrqpc *NewRelicQueryPerformanceCollector) recordDetailedExecutionPlanMetrics(plan *ExecutionPlan, now pcommon.Timestamp) {
	// Recursively traverse the execution plan tree to extract all nodes
	nrqpc.recordPlanNodeMetrics(plan.Plan, plan.DatabaseName, plan.QueryID, now)
}

// recordPlanNodeMetrics recursively processes execution plan nodes
func (nrqpc *NewRelicQueryPerformanceCollector) recordPlanNodeMetrics(node map[string]interface{}, databaseName, queryID string, now pcommon.Timestamp) {
	planData, ok := node["Plan"].(map[string]interface{})
	if !ok {
		// If no "Plan" key, the node itself might be the plan data
		planData = node
	}

	nodeType, ok := planData["Node Type"].(string)
	if !ok {
		return
	}

	nrqpc.logger.Debug("Recording execution plan metrics", 
		zap.String("database", databaseName),
		zap.String("query_id", queryID),
		zap.String("node_type", nodeType),
	)

	// Record existing metrics
	if parallelAware, ok := planData["Parallel Aware"].(bool); ok {
		parallelAwareInt := int64(0)
		if parallelAware {
			parallelAwareInt = 1
		}
		nrqpc.mb.RecordPostgresqlExecutionPlanParallelAwareDataPoint(now, parallelAwareInt, databaseName, queryID, nodeType)
	}

	if asyncCapable, ok := planData["Async Capable"].(bool); ok {
		asyncCapableInt := int64(0)
		if asyncCapable {
			asyncCapableInt = 1
		}
		nrqpc.mb.RecordPostgresqlExecutionPlanAsyncCapableDataPoint(now, asyncCapableInt, databaseName, queryID, nodeType)
	}

	if actualRows, ok := planData["Actual Rows"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanActualRowsDataPoint(now, int64(actualRows), databaseName, queryID, nodeType)
	}

	if actualLoops, ok := planData["Actual Loops"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanActualLoopsDataPoint(now, int64(actualLoops), databaseName, queryID, nodeType)
	}

	if actualTotalTime, ok := planData["Actual Total Time"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanActualTotalTimeDataPoint(now, actualTotalTime, databaseName, queryID, nodeType)
	}

	// Record new detailed metrics
	if totalCost, ok := planData["Total Cost"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanCostEstimateDataPoint(now, totalCost, databaseName, queryID, nodeType)
	}

	if startupCost, ok := planData["Startup Cost"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanStartupTimeDataPoint(now, startupCost, databaseName, queryID, nodeType)
	}

	if planRows, ok := planData["Plan Rows"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanPlanRowsDataPoint(now, int64(planRows), databaseName, queryID, nodeType)
	}

	if planWidth, ok := planData["Plan Width"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanPlanWidthDataPoint(now, int64(planWidth), databaseName, queryID, nodeType)
	}

	// Record I/O timing metrics
	if ioReadTime, ok := planData["I/O Read Time"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanIoReadTimeDataPoint(now, ioReadTime, databaseName, queryID, nodeType)
	}

	if ioWriteTime, ok := planData["I/O Write Time"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanIoWriteTimeDataPoint(now, ioWriteTime, databaseName, queryID, nodeType)
	}

	// Record buffer usage metrics
	if sharedHitBlocks, ok := planData["Shared Hit Blocks"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanSharedHitBlocksDataPoint(now, int64(sharedHitBlocks), databaseName, queryID, nodeType)
	}

	if sharedReadBlocks, ok := planData["Shared Read Blocks"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanSharedReadBlocksDataPoint(now, int64(sharedReadBlocks), databaseName, queryID, nodeType)
	}

	if sharedWrittenBlocks, ok := planData["Shared Written Blocks"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanSharedWrittenBlocksDataPoint(now, int64(sharedWrittenBlocks), databaseName, queryID, nodeType)
	}

	if tempReadBlocks, ok := planData["Temp Read Blocks"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanTempReadBlocksDataPoint(now, int64(tempReadBlocks), databaseName, queryID, nodeType)
	}

	if tempWrittenBlocks, ok := planData["Temp Written Blocks"].(float64); ok {
		nrqpc.mb.RecordPostgresqlExecutionPlanTempWrittenBlocksDataPoint(now, int64(tempWrittenBlocks), databaseName, queryID, nodeType)
	}

	// Recursively process child plans
	if plans, ok := planData["Plans"].([]interface{}); ok {
		for _, childPlan := range plans {
			if childPlanMap, ok := childPlan.(map[string]interface{}); ok {
				nrqpc.recordPlanNodeMetrics(childPlanMap, databaseName, queryID, now)
			}
		}
	}
}

// generateSlowQueryLog generates a detailed log entry for slow queries including execution plan info
func (nrqpc *NewRelicQueryPerformanceCollector) generateSlowQueryLog(slowQuery SlowQueryInfo) {
	logRecord := plog.NewLogRecord()
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	logRecord.SetSeverityNumber(plog.SeverityNumberWarn)
	logRecord.SetSeverityText("WARN")
	
	// Enhanced log body with execution plan summary
	executionPlan := nrqpc.executionPlans[slowQuery.QueryID]
	var logBody strings.Builder
	logBody.WriteString(fmt.Sprintf("Slow query detected: %s", slowQuery.QueryText))
	
	if executionPlan != nil {
		logBody.WriteString(fmt.Sprintf("\nExecution Plan Available: %s", executionPlan.PlanID))
		if planData, ok := executionPlan.Plan["Plan"].(map[string]interface{}); ok {
			if nodeType, ok := planData["Node Type"].(string); ok {
				logBody.WriteString(fmt.Sprintf("\nTop-level Node Type: %s", nodeType))
			}
			if totalCost, ok := planData["Total Cost"].(float64); ok {
				logBody.WriteString(fmt.Sprintf("\nEstimated Total Cost: %.2f", totalCost))
			}
			if planRows, ok := planData["Plan Rows"].(float64); ok {
				logBody.WriteString(fmt.Sprintf("\nEstimated Rows: %.0f", planRows))
			}
		}
	}
	
	body := logRecord.Body()
	body.SetStr(logBody.String())
	
	attrs := logRecord.Attributes()
	attrs.PutStr("postgresql.query.id", slowQuery.QueryID)
	attrs.PutStr("postgresql.query.text", slowQuery.QueryText)
	attrs.PutStr("postgresql.database.name", slowQuery.DatabaseName)
	attrs.PutStr("postgresql.schema.name", slowQuery.SchemaName)
	attrs.PutStr("postgresql.statement.type", slowQuery.StatementType)
	attrs.PutInt("postgresql.query.execution.count", slowQuery.ExecutionCount)
	attrs.PutDouble("postgresql.query.avg_elapsed_time", slowQuery.AvgElapsedTimeMs)
	attrs.PutDouble("postgresql.query.avg_disk_reads", slowQuery.AvgDiskReads)
	attrs.PutDouble("postgresql.query.avg_disk_writes", slowQuery.AvgDiskWrites)
	
	// Add execution plan details to log attributes
	if executionPlan != nil {
		attrs.PutStr("postgresql.execution_plan.id", executionPlan.PlanID)
		attrs.PutStr("postgresql.execution_plan.created_at", executionPlan.CreatedAt.Format(time.RFC3339))
		
		if planData, ok := executionPlan.Plan["Plan"].(map[string]interface{}); ok {
			if nodeType, ok := planData["Node Type"].(string); ok {
				attrs.PutStr("postgresql.execution_plan.top_node_type", nodeType)
			}
			if totalCost, ok := planData["Total Cost"].(float64); ok {
				attrs.PutDouble("postgresql.execution_plan.total_cost", totalCost)
			}
			if startupCost, ok := planData["Startup Cost"].(float64); ok {
				attrs.PutDouble("postgresql.execution_plan.startup_cost", startupCost)
			}
			if planRows, ok := planData["Plan Rows"].(float64); ok {
				attrs.PutInt("postgresql.execution_plan.estimated_rows", int64(planRows))
			}
			if planWidth, ok := planData["Plan Width"].(float64); ok {
				attrs.PutInt("postgresql.execution_plan.estimated_width", int64(planWidth))
			}
			if parallelAware, ok := planData["Parallel Aware"].(bool); ok {
				attrs.PutBool("postgresql.execution_plan.parallel_aware", parallelAware)
			}
			if asyncCapable, ok := planData["Async Capable"].(bool); ok {
				attrs.PutBool("postgresql.execution_plan.async_capable", asyncCapable)
			}
		}
	}
	
	// Append the log record to the logs builder
	nrqpc.lb.AppendLogRecord(logRecord)
}

// formatDatabaseList formats the database list for SQL IN clause
func (nrqpc *NewRelicQueryPerformanceCollector) formatDatabaseList(databases []string) string {
	quotedDatabases := make([]string, len(databases))
	for i, db := range databases {
		quotedDatabases[i] = "'" + strings.ReplaceAll(db, "'", "''") + "'"
	}
	return strings.Join(quotedDatabases, ", ")
}

// shouldSkipExecutionPlan determines if a query should be skipped for execution plan generation
func (nrqpc *NewRelicQueryPerformanceCollector) shouldSkipExecutionPlan(queryText string) bool {
	upperQuery := strings.ToUpper(strings.TrimSpace(queryText))
	
	// Skip utility commands that can't be explained
	skipPatterns := []string{
		"ANALYZE",
		"VACUUM",
		"REINDEX",
		"CLUSTER",
		"TRUNCATE",
		"DROP",
		"CREATE INDEX",
		"ALTER",
		"GRANT",
		"REVOKE",
		"COMMENT",
		"SET ",
		"SHOW ",
		"RESET",
	}
	
	for _, pattern := range skipPatterns {
		if strings.HasPrefix(upperQuery, pattern) {
			return true
		}
	}
	
	return false
}

// cleanQueryForExplain cleans query text to make it suitable for EXPLAIN
func (nrqpc *NewRelicQueryPerformanceCollector) cleanQueryForExplain(queryText string) string {
	// Replace common parameter placeholders with dummy values
	cleaned := queryText
	
	// Replace $1, $2, etc. with dummy values
	for i := 1; i <= 20; i++ {
		placeholder := fmt.Sprintf("$%d", i)
		if strings.Contains(cleaned, placeholder) {
			// Use a reasonable dummy value based on context
			cleaned = strings.ReplaceAll(cleaned, placeholder, "1")
		}
	}
	
	// Replace ? placeholders with dummy values
	cleaned = strings.ReplaceAll(cleaned, "?", "1")
	
	return cleaned
}
