//! Arrow segment rotation and time-bounds pruning.
#[path = "support/arrow.rs"]
mod arrow;
use arrow::{every_write, reading, readings, select, window};
use database::arrow::{Bounds, Rotation, segments};

#[tokio::test]
async fn rotation_closes_segments_with_bounds_and_reads_span_them() {
    let (provider, db) = select("rotate", every_write()).await;
    let series = &db.series;
    let batch = &db.batch;
    for (i, at) in [30, 10, 20].into_iter().enumerate() {
        let rows = [
            reading("zat", i as f64, 1, at),
            reading("sat", 0.0, 1, at + 5),
        ];
        batch.create_many(&readings(), &rows).await.unwrap();
    }
    let files = segments(&provider.table_dir("bms", "readings")).unwrap();
    assert_eq!(files.len(), 3, "every write reached the 1-byte limit");
    let bounds: Vec<Bounds> = files.iter().map(|f| Bounds::read(f).unwrap()).collect();
    assert_eq!(
        bounds[0],
        Bounds {
            min: 30,
            max: 35,
            rows: 2
        }
    );
    assert_eq!(
        bounds[1],
        Bounds {
            min: 10,
            max: 15,
            rows: 2
        }
    );

    let times: Vec<i64> = series
        .range(&readings(), &window(0, 100))
        .await
        .unwrap()
        .items
        .iter()
        .map(|r| r["observed_at"].as_i64().unwrap())
        .collect();
    assert_eq!(times, [10, 15, 20, 25, 30, 35]);
}

#[tokio::test]
async fn files_outside_the_window_are_never_opened() {
    let (provider, db) = select("prune", every_write()).await;
    let series = &db.series;
    let batch = &db.batch;
    batch
        .create_many(&readings(), &[reading("zat", 1.0, 1, 10)])
        .await
        .unwrap();
    batch
        .create_many(&readings(), &[reading("zat", 2.0, 1, 1_000)])
        .await
        .unwrap();
    let files = segments(&provider.table_dir("bms", "readings")).unwrap();
    // Corrupt the early segment: only a query whose window reaches it can fail.
    std::fs::write(&files[0], b"not arrow").unwrap();

    let late = series
        .range(&readings(), &window(500, 2_000))
        .await
        .unwrap();
    assert_eq!(late.items.len(), 1);
    assert!(series.range(&readings(), &window(0, 2_000)).await.is_err());
}

#[tokio::test]
async fn the_open_segment_has_no_bounds_and_is_still_read() {
    let (provider, db) = select("open", Rotation::never()).await;
    let series = &db.series;
    let batch = &db.batch;
    batch
        .create_many(&readings(), &[reading("zat", 1.0, 1, 10)])
        .await
        .unwrap();
    let files = segments(&provider.table_dir("bms", "readings")).unwrap();
    assert!(Bounds::read(&files[0]).is_none());
    assert_eq!(
        series
            .range(&readings(), &window(0, 20))
            .await
            .unwrap()
            .items
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_never_see_a_segment_before_its_header() {
    use database::store::ListOptions;
    let (_provider, db) = select("concurrent", every_write()).await;
    let reader = {
        let db = db.clone();
        tokio::spawn(async move {
            let mut reads = 0;
            loop {
                let n = db
                    .count(&readings(), &ListOptions::default())
                    .await
                    .unwrap();
                reads += 1;
                if n == 200 {
                    return reads;
                }
            }
        })
    };
    for at in 0..200 {
        db.batch
            .create_many(&readings(), &[reading("zat", 1.0, 1, at)])
            .await
            .unwrap();
    }
    assert!(
        reader.await.unwrap() > 1,
        "the reader overlapped the writes"
    );
}
