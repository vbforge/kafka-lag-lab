<#
.SYNOPSIS
  kafka-lag-lab :: run-scenario.ps1
  One script, three roles (producer / consumer / diagnose), N named scenarios.
  Run the SAME scenario name in separate terminals — one per role.

.EXAMPLE
  # Terminal 1
  .\scripts\run-scenario.ps1 -Role consumer -Scenario retry-storm

  # Terminal 2
  .\scripts\run-scenario.ps1 -Role producer -Scenario retry-storm

  # Terminal 3 (optional)
  .\scripts\run-scenario.ps1 -Role diagnose -Scenario retry-storm
#>

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet(
            "baseline",
            "hot-partition", "hot-partition-fixed",
            "slow-processing", "slow-processing-fixed",
            "retry-storm", "retry-storm-fixed",
            "rebalance-thrashing", "rebalance-thrashing-fixed"
    )]
    [string]$Scenario,

    [Parameter(Mandatory=$true)]
    [ValidateSet("producer", "consumer", "diagnose")]
    [string]$Role
)

# JUNIOR NOTE: single source of truth for "what does scenario X actually
# set" per role — same spirit as the Java factories, just for shell config.

$ProducerEnv = @{
    "baseline"                   = @{ KEY_STRATEGY = "uniform" }
    "hot-partition"              = @{ KEY_STRATEGY = "hotkey" }
    "hot-partition-fixed"        = @{ KEY_STRATEGY = "hotkey-salted"; KEY_SALT_BUCKETS = "6" }
    "slow-processing"            = @{ KEY_STRATEGY = "uniform" }
    "slow-processing-fixed"      = @{ KEY_STRATEGY = "uniform" }
    "retry-storm"                = @{ KEY_STRATEGY = "uniform" }
    "retry-storm-fixed"          = @{ KEY_STRATEGY = "uniform" }
    "rebalance-thrashing"        = @{ KEY_STRATEGY = "uniform" }
    "rebalance-thrashing-fixed"  = @{ KEY_STRATEGY = "uniform" }
}

$ConsumerEnv = @{
    "baseline"                   = @{ PROCESSING_MODE = "baseline" }
    "hot-partition"              = @{ PROCESSING_MODE = "baseline" }
    "hot-partition-fixed"        = @{ PROCESSING_MODE = "baseline" }
    "slow-processing"            = @{ PROCESSING_MODE = "slow-sync"; SLOW_DOWNSTREAM_MS = "20" }
    "slow-processing-fixed"      = @{ PROCESSING_MODE = "async-batch"; SLOW_DOWNSTREAM_MS = "20"; THREAD_POOL_SIZE = "16" }
    "retry-storm"                = @{ PROCESSING_MODE = "retry-storm"; RETRY_FAILURE_RATE = "0.7" }
    "retry-storm-fixed"          = @{ PROCESSING_MODE = "retry-bounded"; RETRY_FAILURE_RATE = "0.3"; RETRY_MAX_ATTEMPTS = "3"; RETRY_BACKOFF_MS = "200" }
    "rebalance-thrashing"        = @{ PROCESSING_MODE = "slow-sync"; SLOW_DOWNSTREAM_MS = "20"; MAX_POLL_INTERVAL_MS = "6000" }
    "rebalance-thrashing-fixed"  = @{ PROCESSING_MODE = "async-batch"; SLOW_DOWNSTREAM_MS = "20"; THREAD_POOL_SIZE = "16"; MAX_POLL_INTERVAL_MS = "6000" }
}

function Set-ScenarioEnv($envMap, $scenario) {
    if (-not $envMap.ContainsKey($scenario)) { return }
    foreach ($kv in $envMap[$scenario].GetEnumerator()) {
        Set-Item -Path "Env:$($kv.Key)" -Value $kv.Value
        Write-Host "  $($kv.Key)=$($kv.Value)"
    }
}

Write-Host "=== kafka-lag-lab :: $Role :: $Scenario ===" -ForegroundColor Cyan

switch ($Role) {
    "producer" {
        Set-ScenarioEnv $ProducerEnv $Scenario
        java -jar producer/target/producer-1.0-SNAPSHOT.jar
    }
    "consumer" {
        Set-ScenarioEnv $ConsumerEnv $Scenario
        # JUNIOR NOTE: Tee-Object mirrors output to console AND a log file —
        # the diagnose module needs that file to detect [REBALANCE] lines.
        java -jar consumer/target/consumer-1.0-SNAPSHOT.jar | Tee-Object -FilePath "consumer.log"
    }
    "diagnose" {
        $env:REBALANCE_LOG = "consumer.log"
        java -jar diagnose/target/diagnose-1.0-SNAPSHOT.jar
    }
}