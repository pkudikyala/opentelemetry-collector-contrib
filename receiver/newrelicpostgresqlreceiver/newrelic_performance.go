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
	logger          *zap.Logger
	mb              *metadata.MetricsBuilder
	lb              *metadata.LogsBuilder
	config          *NewRelicQueryPerformanceConfig
	client          client
	slowQueryCache  map[string]*SlowQueryInfo
	executionPlans  map[string]*ExecutionPlan
	lastCollectedAt time.Time
}

// SlowQueryInfo represents information about a slow query
type SlowQueryInfo struct {
	QueryID          string    `json:"query_id"`
	QueryText        string    `json:"query_text"`
	DatabaseName     string    `json:"database_name"`
	SchemaName       string    `json:"schema_name"`
	ExecutionCount   int64     `json:"execution_count"`
	AvgElapsedTimeMs float64   `json:"avg_elapsed_time_ms"`
	AvgDiskReads     float64   `json:"avg_disk_reads"`
	AvgDiskWrites    float64   `json:"avg_disk_writes"`
	StatementType    string    `json:"statement_type"`
	CollectionTime   time.Time `json:"collection_time"`
	IndividualQuery  string    `json:"individual_query,omitempty"`
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
	QueryID       string  `json:"query_id"`
	QueryText     string  `json:"query_text"`
	DatabaseName  string  `json:"database_name"`
	PlanID        string  `json:"plan_id"`
	CPUTimeMs     float64 `json:"cpu_time_ms"`
	ExecTimeMs    float64 `json:"exec_time_ms"`
	RealQueryText string  `json:"real_query_text"`
}

// NewRelicQueryPerformanceCollector creates a new instance of the collector
func NewNewRelicQueryPerformanceCollector(logger *zap.Logger, mb *metadata.MetricsBuilder, lb *metadata.LogsBuilder, config *NewRelicQueryPerformanceConfig, client client) *NewRelicQueryPerformanceCollector {
	return &NewRelicQueryPerformanceCollector{
		logger:          logger,
		mb:              mb,
		lb:              lb,
		config:          config,
		client:          client,
		slowQueryCache:  make(map[string]*SlowQueryInfo),
		executionPlans:  make(map[string]*ExecutionPlan),
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
			current_schema() AS schema_name,
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
		5, // minimum calls
		nrqpc.config.SlowQueryThresholdMs,
		nrqpc.config.MaxQueriesPerCollection,
	)

	rows, err := nrqpc.client.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute slow queries query: %w", err)
	}
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())

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
		nrqpc.logger.Info("Slow query details",
			zap.String("query_id", slowQuery.QueryID),
			zap.String("query_text", slowQuery.QueryText),
			zap.String("database_name", slowQuery.DatabaseName),
			zap.String("schema_name", slowQuery.SchemaName),
			zap.Int64("execution_count", slowQuery.ExecutionCount),
			zap.Float64("avg_elapsed_time_ms", slowQuery.AvgElapsedTimeMs),
			zap.Float64("avg_disk_reads", slowQuery.AvgDiskReads),
			zap.Float64("avg_disk_writes", slowQuery.AvgDiskWrites),
			zap.String("statement_type", slowQuery.StatementType),
			zap.Time("collection_time", slowQuery.CollectionTime),
		)

		if err != nil {
			nrqpc.logger.Error("Failed to scan slow query row", zap.Error(err))
			continue
		}

		// Cache the slow query for later use
		nrqpc.slowQueryCache[slowQuery.QueryID] = &slowQuery

		// Record metrics
		nrqpc.mb.RecordPostgresqlQueryExecutionCountDataPoint(now, slowQuery.ExecutionCount, slowQuery.DatabaseName, slowQuery.QueryID, slowQuery.QueryText, slowQuery.StatementType)
		nrqpc.mb.RecordPostgresqlQueryAvgElapsedTimeDataPoint(now, slowQuery.AvgElapsedTimeMs, slowQuery.DatabaseName, slowQuery.QueryID, slowQuery.QueryText, slowQuery.StatementType)
		nrqpc.mb.RecordPostgresqlQueryAvgDiskReadsDataPoint(now, slowQuery.AvgDiskReads, slowQuery.DatabaseName, slowQuery.QueryID, slowQuery.QueryText, slowQuery.StatementType)
		nrqpc.mb.RecordPostgresqlQueryAvgDiskWritesDataPoint(now, slowQuery.AvgDiskWrites, slowQuery.DatabaseName, slowQuery.QueryID, slowQuery.QueryText, slowQuery.StatementType)

		// Generate log entry for slow query
		nrqpc.generateSlowQueryLog(slowQuery)
	}

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
		nrqpc.mb.RecordPostgresqlQueryCPUTimeDataPoint(now, individualQuery.CPUTimeMs, individualQuery.DatabaseName, individualQuery.QueryID, individualQuery.QueryText)
	}

	return nil
}

// collectExecutionPlans collects execution plans for queries
func (nrqpc *NewRelicQueryPerformanceCollector) collectExecutionPlans(ctx context.Context, databases []string) error {
	// For each slow query in cache, try to get execution plan
	for queryID, slowQuery := range nrqpc.slowQueryCache {
		if _, exists := nrqpc.executionPlans[queryID]; exists {
			continue // Already have plan for this query
		}

		// Get execution plan using EXPLAIN
		planQuery := fmt.Sprintf("EXPLAIN (FORMAT JSON) %s", slowQuery.QueryText)

		rows, err := nrqpc.client.Query(ctx, planQuery)
		if err != nil {
			nrqpc.logger.Debug("Failed to get execution plan", zap.String("query_id", queryID), zap.Error(err))
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
			nrqpc.recordExecutionPlanMetrics(executionPlan)
		}
	}

	return nil
}

// recordExecutionPlanMetrics records execution plan related metrics
func (nrqpc *NewRelicQueryPerformanceCollector) recordExecutionPlanMetrics(plan *ExecutionPlan) {
	now := pcommon.NewTimestampFromTime(time.Now())

	// Extract plan information and record metrics
	if planData, ok := plan.Plan["Plan"].(map[string]interface{}); ok {
		if nodeType, ok := planData["Node Type"].(string); ok {
			// Record parallel aware
			if parallelAware, ok := planData["Parallel Aware"].(bool); ok {
				parallelAwareInt := int64(0)
				if parallelAware {
					parallelAwareInt = 1
				}
				nrqpc.mb.RecordPostgresqlExecutionPlanParallelAwareDataPoint(now, parallelAwareInt, plan.DatabaseName, plan.QueryID, nodeType)
			}

			// Record async capable
			if asyncCapable, ok := planData["Async Capable"].(bool); ok {
				asyncCapableInt := int64(0)
				if asyncCapable {
					asyncCapableInt = 1
				}
				nrqpc.mb.RecordPostgresqlExecutionPlanAsyncCapableDataPoint(now, asyncCapableInt, plan.DatabaseName, plan.QueryID, nodeType)
			}

			// Record actual rows
			if actualRows, ok := planData["Actual Rows"].(float64); ok {
				nrqpc.mb.RecordPostgresqlExecutionPlanActualRowsDataPoint(now, int64(actualRows), plan.DatabaseName, plan.QueryID, nodeType)
			}

			// Record actual loops
			if actualLoops, ok := planData["Actual Loops"].(float64); ok {
				nrqpc.mb.RecordPostgresqlExecutionPlanActualLoopsDataPoint(now, int64(actualLoops), plan.DatabaseName, plan.QueryID, nodeType)
			}

			// Record actual total time
			if actualTotalTime, ok := planData["Actual Total Time"].(float64); ok {
				nrqpc.mb.RecordPostgresqlExecutionPlanActualTotalTimeDataPoint(now, actualTotalTime, plan.DatabaseName, plan.QueryID, nodeType)
			}
		}
	}
}

// generateSlowQueryLog generates a log entry for slow queries
func (nrqpc *NewRelicQueryPerformanceCollector) generateSlowQueryLog(slowQuery SlowQueryInfo) {
	logRecord := plog.NewLogRecord()
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	logRecord.SetSeverityNumber(plog.SeverityNumberWarn)
	logRecord.SetSeverityText("WARN")

	body := logRecord.Body()
	body.SetStr(fmt.Sprintf("Slow query detected: %s", slowQuery.QueryText))

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
