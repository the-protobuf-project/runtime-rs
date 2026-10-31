//! Application-owned exporter setup; library instrumentation never installs it.
//! Enable telemetry-sdk to export all three signals to localhost:16009.
use super::Result;
use telemetry::Telemetry;

pub struct Session {
    signals: Telemetry,
    #[cfg(feature = "telemetry-sdk")]
    owner: telemetry::sdk::Telemetry,
}
impl Session {
    pub fn new() -> Result<Self> {
        #[cfg(feature = "telemetry-sdk")]
        {
            let port = std::env::var("OTEL_COLLECTOR_PORT")
                .unwrap_or_else(|_| "16009".into())
                .parse::<u16>()?;
            let owner = telemetry::sdk::Telemetry::new()
                .with_service("runtime-influxdb-example", env!("CARGO_PKG_VERSION"))
                .environment(telemetry::sdk::Environment::Development)
                .with_local_otel_collector()
                .with_otlp("127.0.0.1", port)
                .with_tracing()
                .build()?;
            let signals = Telemetry::from_tpp(&owner, "runtime.database");
            Ok(Self { signals, owner })
        }
        #[cfg(not(feature = "telemetry-sdk"))]
        {
            Ok(Self {
                signals: Telemetry::default(),
            })
        }
    }
    pub fn signals(&self) -> Telemetry {
        self.signals.clone()
    }
    pub async fn close(self) -> Result<()> {
        #[cfg(feature = "telemetry-sdk")]
        {
            let mut owner = self.owner;
            // SDK shutdown can block while it flushes; never block a Tokio worker.
            tokio::task::spawn_blocking(move || owner.close()).await??;
        }
        Ok(())
    }
}
