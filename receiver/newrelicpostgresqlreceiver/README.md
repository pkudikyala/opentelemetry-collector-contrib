# New Relic PostgreSQL Receiver

The New Relic PostgreSQL receiver connects to PostgreSQL instances and collects performance metrics and logs specifically tailored for New Relic-style monitoring and query performance analysis.

This receiver extends the standard PostgreSQL receiver with enhanced query performance monitoring capabilities inspired by New Relic's nri-postgresql integration.

## Supported pipeline types

- `metrics`
- `logs`

## Getting Started

All that is required to enable the New Relic PostgreSQL receiver is to include it in the receiver definitions. A simple configuration with only the required `username` and `password` is shown below:

```yaml
receivers:
  newrelicpostgresql:
    endpoint: localhost:5432
    transport: tcp
    username: otel
    password: ${env:POSTGRESQL_PASSWORD}
    databases:
      - mydb
    newrelic_query_performance:
      enabled: true
      slow_query_threshold_ms: 1000
      max_queries_per_collection: 100
      query_monitoring_enabled: true
      wait_event_monitoring_enabled: true
      blocking_sessions_enabled: true
      execution_plan_enabled: true
      individual_query_enabled: true
```

## Configuration

The New Relic PostgreSQL receiver supports all the same configuration options as the standard PostgreSQL receiver, plus additional New Relic-specific options:

### Basic Configuration

| Parameter | Default | Required | Description |
|-----------|---------|----------|-------------|
| `endpoint` | `localhost:5432` | false | The endpoint of the PostgreSQL server |
| `transport` | `tcp` | false | The transport protocol to use (`tcp` or `unix`) |
| `username` | | true | The username used to authenticate |
| `password` | | true | The password used to authenticate |
| `databases` | `[]` | false | List of database names to monitor. If empty, all databases will be monitored |
| `exclude_databases` | `[]` | false | List of database names to exclude from monitoring |

### New Relic Query Performance Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `newrelic_query_performance.enabled` | `true` | Enable New Relic query performance monitoring |
| `newrelic_query_performance.slow_query_threshold_ms` | `1000` | Minimum execution time in milliseconds to consider a query as slow |
| `newrelic_query_performance.max_queries_per_collection` | `100` | Maximum number of queries to collect per monitoring cycle |
| `newrelic_query_performance.query_monitoring_enabled` | `true` | Enable slow query monitoring |
| `newrelic_query_performance.wait_event_monitoring_enabled` | `true` | Enable wait event monitoring |
| `newrelic_query_performance.blocking_sessions_enabled` | `true` | Enable blocking sessions monitoring |
| `newrelic_query_performance.execution_plan_enabled` | `true` | Enable query execution plan monitoring |
| `newrelic_query_performance.query_plan_cache_size` | `1000` | Number of execution plans to cache |
| `newrelic_query_performance.query_plan_cache_ttl` | `1h` | Time to live for cached execution plans |
| `newrelic_query_performance.individual_query_enabled` | `true` | Enable individual query monitoring |

### TLS Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `tls.insecure` | `false` | Whether to enable client transport security |
| `tls.insecure_skip_verify` | `false` | Whether to skip verifying the certificate |
| `tls.ca_file` | | Path to the CA cert for the server |
| `tls.cert_file` | | Path to the TLS cert to use for client connections |
| `tls.key_file` | | Path to the TLS key to use for client connections |

### Connection Pool Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `connection_pool.max_idle_time` | `0` | Maximum amount of time a connection may be idle |
| `connection_pool.max_lifetime` | `0` | Maximum amount of time a connection may be reused |
| `connection_pool.max_idle` | `2` | Maximum number of idle connections in the pool |
| `connection_pool.max_open` | `0` | Maximum number of open connections to the database |

## Metrics

The New Relic PostgreSQL receiver collects all standard PostgreSQL metrics plus additional New Relic-specific query performance metrics:

### Standard PostgreSQL Metrics

- `postgresql.blocks_read`
- `postgresql.blocks_hit`
- `postgresql.operations`
- `postgresql.rows`
- `postgresql.commits`
- `postgresql.rollbacks`
- `postgresql.connection.max`
- `postgresql.connection.count`
- `postgresql.database.count`
- `postgresql.database.locks`
- `postgresql.index.scans`
- `postgresql.index.size`
- `postgresql.table.scans`
- `postgresql.table.size`
- `postgresql.table.vacuum.count`
- `postgresql.wal.age`
- `postgresql.wal.lag`

### New Relic Query Performance Metrics

- `postgresql.query.execution.count` - Number of times a query was executed
- `postgresql.query.avg_elapsed_time` - Average execution time for queries in milliseconds
- `postgresql.query.avg_disk_reads` - Average number of disk reads per query execution
- `postgresql.query.avg_disk_writes` - Average number of disk writes per query execution
- `postgresql.query.cpu_time` - CPU time consumed by queries in milliseconds
- `postgresql.wait.event.total_time` - Total wait time for wait events in milliseconds
- `postgresql.blocking.session.pid` - Process ID of blocking sessions
- `postgresql.blocked.session.pid` - Process ID of blocked sessions
- `postgresql.execution_plan.parallel_aware` - Whether execution plan nodes are parallel aware
- `postgresql.execution_plan.async_capable` - Whether execution plan nodes are async capable
- `postgresql.execution_plan.actual_rows` - Actual number of rows processed by execution plan nodes
- `postgresql.execution_plan.actual_loops` - Number of times execution plan nodes were executed
- `postgresql.execution_plan.actual_total_time` - Total time spent in execution plan nodes

## Logs

The receiver generates logs for:

- Slow queries that exceed the configured threshold
- Query performance monitoring events
- Error conditions

Each log entry includes relevant database context such as database name, query ID, execution time, and query text.

## Prerequisites

This receiver requires:

1. PostgreSQL 12 or higher
2. The `pg_stat_statements` extension enabled for query statistics
3. Appropriate user permissions to access system catalogs and statistics views

### Required PostgreSQL Extensions

```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

### Required Permissions

The monitoring user needs the following permissions:

```sql
-- Basic monitoring permissions
GRANT SELECT ON pg_stat_database TO monitoring_user;
GRANT SELECT ON pg_stat_user_tables TO monitoring_user;
GRANT SELECT ON pg_stat_user_indexes TO monitoring_user;
GRANT SELECT ON pg_stat_activity TO monitoring_user;
GRANT SELECT ON pg_stat_statements TO monitoring_user;
GRANT SELECT ON pg_locks TO monitoring_user;
GRANT SELECT ON pg_database TO monitoring_user;
GRANT SELECT ON pg_stat_bgwriter TO monitoring_user;
GRANT SELECT ON pg_stat_wal_receiver TO monitoring_user;

-- For execution plan monitoring
GRANT EXECUTE ON FUNCTION pg_stat_statements_reset() TO monitoring_user;
```

## New Relic Integration Features

This receiver implements monitoring capabilities similar to New Relic's nri-postgresql integration:

1. **Slow Query Monitoring**: Identifies and monitors queries that exceed performance thresholds
2. **Wait Event Analysis**: Tracks database wait events and categorizes them (Locks, Disk IO, CPU, Other)
3. **Blocking Session Detection**: Identifies sessions that are blocking other sessions
4. **Execution Plan Analysis**: Collects and analyzes query execution plans
5. **Individual Query Tracking**: Monitors currently executing queries with detailed metrics

## Example Configuration

```yaml
receivers:
  newrelicpostgresql:
    endpoint: localhost:5432
    transport: tcp
    username: monitoring_user
    password: ${env:POSTGRESQL_PASSWORD}
    databases:
      - production_db
      - analytics_db
    exclude_databases:
      - template0
      - template1
    
    # New Relic specific configuration
    newrelic_query_performance:
      enabled: true
      slow_query_threshold_ms: 500
      max_queries_per_collection: 50
      query_monitoring_enabled: true
      wait_event_monitoring_enabled: true
      blocking_sessions_enabled: true
      execution_plan_enabled: true
      query_plan_cache_size: 500
      query_plan_cache_ttl: 30m
      individual_query_enabled: true
    
    # TLS configuration
    tls:
      insecure: false
      ca_file: /etc/ssl/certs/ca-certificates.crt
      cert_file: /etc/ssl/client.crt
      key_file: /etc/ssl/client.key
    
    # Connection pool configuration
    connection_pool:
      max_idle_time: 10m
      max_lifetime: 1h
      max_idle: 5
      max_open: 20
    
    # Collection configuration
    collection_interval: 60s

processors:
  batch:

exporters:
  logging:
    loglevel: debug

service:
  pipelines:
    metrics:
      receivers: [newrelicpostgresql]
      processors: [batch]
      exporters: [logging]
    logs:
      receivers: [newrelicpostgresql]
      processors: [batch]
      exporters: [logging]
```

This configuration will collect PostgreSQL metrics and logs with New Relic-style query performance monitoring, providing deep insights into database performance and query execution patterns.
