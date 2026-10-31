//! A simulated BMS point feed, written as generated client code: one
//! `create` per reading. Replace `read` with real BACnet reads.
use database::store::{Column, Error, For, Kind, Model, Record, Resource, Role};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// One present-value reading from one BACnet object, as the generator emits.
pub struct Point {
    pub run: String,
    pub device: String,
    pub object: String,
    pub value: f64,
    pub observed_at: i64,
}

static POINT: LazyLock<Resource> = LazyLock::new(|| {
    Resource::new(
        "points",
        vec![
            Column::new("run", Kind::String, Role::Tag),
            Column::new("device", Kind::String, Role::Tag),
            Column::new("object", Kind::String, Role::Tag),
            Column::new("value", Kind::Float, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .expect("generated descriptor is valid")
    .with_name("Point")
});

impl Model for Point {
    fn resource() -> &'static Resource {
        &POINT
    }
    fn to_record(&self) -> database::Result<Record> {
        Ok(Record::from([
            ("run".into(), self.run.as_str().into()),
            ("device".into(), self.device.as_str().into()),
            ("object".into(), self.object.as_str().into()),
            ("value".into(), self.value.into()),
            ("observed_at".into(), self.observed_at.into()),
        ]))
    }
    fn from_record(_: Record) -> database::Result<Self> {
        Err(Error::Invalid("the capture only writes".into()))
    }
}

/// Producer `lane` of `lanes`: 2,000 objects split across lanes, each reading
/// `rate / lanes` points a second in 10 ms ticks until `stop`. Every point in
/// a tick gets its own nanosecond, so InfluxDB never merges two readings.
pub async fn run(
    points: For<Point>,
    run: String,
    (lane, lanes): (u64, u64),
    rate: u64,
    stop: &AtomicBool,
    sent: &AtomicU64,
) -> database::Result<()> {
    let per_tick = (rate / lanes / 100).max(1);
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    let mut n: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        tick.tick().await;
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        for k in 0..per_tick {
            let object = (n * lanes + lane) % 2_000;
            let point = Point {
                run: run.clone(),
                device: format!("device-{}", object / 100),
                object: format!("analog-input:{object}"),
                value: 20.0 + (n % 100) as f64 / 10.0,
                observed_at: now + (k * lanes + lane) as i64,
            };
            points.create(&point).await?;
            n += 1;
        }
        sent.fetch_add(per_tick, Ordering::Relaxed);
    }
    Ok(())
}
