// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver"

import (
	"errors"
	"fmt"
	"net"
	"time"

	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/multierr"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
)

// Errors for missing required config parameters.
const (
	ErrNoUsername          = "invalid config: missing username"
	ErrNoPassword          = "invalid config: missing password" // #nosec G101 - not hardcoded credentials
	ErrNotSupported        = "invalid config: field '%s' not supported"
	ErrTransportsSupported = "invalid config: 'transport' must be 'tcp' or 'unix'"
	ErrHostPort            = "invalid config: 'endpoint' must be in the form <host>:<port> no matter what 'transport' is configured"
)

// NewRelicQueryPerformanceConfig contains configuration for New Relic query performance monitoring
type NewRelicQueryPerformanceConfig struct {
	Enabled                    bool          `mapstructure:"enabled"`
	SlowQueryThresholdMs       int64         `mapstructure:"slow_query_threshold_ms"`
	MaxQueriesPerCollection    int64         `mapstructure:"max_queries_per_collection"`
	QueryMonitoringEnabled     bool          `mapstructure:"query_monitoring_enabled"`
	WaitEventMonitoringEnabled bool          `mapstructure:"wait_event_monitoring_enabled"`
	BlockingSessionsEnabled    bool          `mapstructure:"blocking_sessions_enabled"`
	ExecutionPlanEnabled       bool          `mapstructure:"execution_plan_enabled"`
	QueryPlanCacheSize         int           `mapstructure:"query_plan_cache_size"`
	QueryPlanCacheTTL          time.Duration `mapstructure:"query_plan_cache_ttl"`
	IndividualQueryEnabled     bool          `mapstructure:"individual_query_enabled"`
}

type TopQueryCollection struct {
	Enabled                bool          `mapstructure:"enabled"`
	MaxRowsPerQuery        int64         `mapstructure:"max_rows_per_query"`
	TopNQuery              int64         `mapstructure:"top_n_query"`
	MaxExplainEachInterval int64         `mapstructure:"max_explain_each_interval"`
	QueryPlanCacheSize     int           `mapstructure:"query_plan_cache_size"`
	QueryPlanCacheTTL      time.Duration `mapstructure:"query_plan_cache_ttl"`
}

type QuerySampleCollection struct {
	Enabled         bool  `mapstructure:"enabled"`
	MaxRowsPerQuery int64 `mapstructure:"max_rows_per_query"`
}

type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	Username                       string                         `mapstructure:"username"`
	Password                       configopaque.String            `mapstructure:"password"`
	Databases                      []string                       `mapstructure:"databases"`
	ExcludeDatabases               []string                       `mapstructure:"exclude_databases"`
	confignet.AddrConfig           `mapstructure:",squash"`       // provides Endpoint and Transport
	configtls.ClientConfig         `mapstructure:"tls,omitempty"` // provides SSL details
	ConnectionPool                 `mapstructure:"connection_pool,omitempty"`
	metadata.MetricsBuilderConfig  `mapstructure:",squash"`
	QuerySampleCollection          `mapstructure:"query_sample_collection,omitempty"`
	TopQueryCollection             `mapstructure:"top_query_collection,omitempty"`
	NewRelicQueryPerformance       NewRelicQueryPerformanceConfig `mapstructure:"newrelic_query_performance,omitempty"`
}

type ConnectionPool struct {
	MaxIdleTime *time.Duration `mapstructure:"max_idle_time,omitempty"`
	MaxLifetime *time.Duration `mapstructure:"max_lifetime,omitempty"`
	MaxIdle     *int           `mapstructure:"max_idle,omitempty"`
	MaxOpen     *int           `mapstructure:"max_open,omitempty"`
}

func (cfg *Config) Validate() error {
	var err error
	if cfg.Username == "" {
		err = multierr.Append(err, errors.New(ErrNoUsername))
	}
	if cfg.Password == "" {
		err = multierr.Append(err, errors.New(ErrNoPassword))
	}

	// The lib/pq module does not support overriding ServerName or specifying supported TLS versions
	if cfg.ServerName != "" {
		err = multierr.Append(err, fmt.Errorf(ErrNotSupported, "ServerName"))
	}
	if cfg.MaxVersion != "" {
		err = multierr.Append(err, fmt.Errorf(ErrNotSupported, "MaxVersion"))
	}
	if cfg.MinVersion != "" {
		err = multierr.Append(err, fmt.Errorf(ErrNotSupported, "MinVersion"))
	}

	switch cfg.Transport {
	case confignet.TransportTypeTCP, confignet.TransportTypeUnix:
		_, _, endpointErr := net.SplitHostPort(cfg.Endpoint)
		if endpointErr != nil {
			err = multierr.Append(err, errors.New(ErrHostPort))
		}
	default:
		err = multierr.Append(err, errors.New(ErrTransportsSupported))
	}

	// Validate New Relic specific configurations
	if cfg.NewRelicQueryPerformance.Enabled {
		if cfg.NewRelicQueryPerformance.SlowQueryThresholdMs <= 0 {
			cfg.NewRelicQueryPerformance.SlowQueryThresholdMs = 1000 // Default to 1 second
		}
		if cfg.NewRelicQueryPerformance.MaxQueriesPerCollection <= 0 {
			cfg.NewRelicQueryPerformance.MaxQueriesPerCollection = 100 // Default to 100 queries
		}
		if cfg.NewRelicQueryPerformance.QueryPlanCacheSize <= 0 {
			cfg.NewRelicQueryPerformance.QueryPlanCacheSize = 1000 // Default cache size
		}
		if cfg.NewRelicQueryPerformance.QueryPlanCacheTTL <= 0 {
			cfg.NewRelicQueryPerformance.QueryPlanCacheTTL = time.Hour // Default TTL
		}
	}

	return err
}
