//! Arrow captures paged with tokens, AIP-160 filters, totals, list and count.
#[path = "support/arrow_points.rs"]
mod points;
use database::arrow::ArrowProvider;
use database::store::{Error, ListOptions};
use points::{Point, open, point, root, window};

#[tokio::test]
async fn ranges_page_with_tokens_filters_and_totals() {
    let provider = ArrowProvider::new(root("query"));
    let points_db = open(&provider).await;
    let batch: Vec<Point> = (0..10).map(|i| point("zat", 20.0 + i as f64, i)).collect();
    points_db.create_many(&batch).await.unwrap();

    let mut range = window(2, 9);
    range.descending = true;
    range.page_size = 2;
    range.filter = "value < 27 AND object = 'zat'".into();
    let (mut times, mut pages) = (Vec::new(), 0);
    loop {
        let page = points_db
            .db()
            .series
            .range(points_db.resource(), &range)
            .await
            .unwrap();
        assert_eq!(page.total, 5, "rows 2..=6 match");
        times.extend(
            page.items
                .iter()
                .map(|r| r["observed_at"].as_i64().unwrap()),
        );
        pages += 1;
        if page.next_page_token.is_empty() {
            break;
        }
        range.page_token = page.next_page_token;
    }
    assert_eq!((times, pages), (vec![6, 5, 4, 3, 2], 3));

    range.filter = "value < 27 OR object = 'zat'".into();
    assert!(matches!(
        points_db.range(&range).await,
        Err(Error::Invalid(_))
    ));
    range.filter = "nonexistent = 1".into();
    assert!(points_db.range(&range).await.is_err());

    let all = ListOptions::default().page(4).order_by("observed_at desc");
    let (page, next, total) = points_db.list(&all).await.unwrap();
    assert_eq!((page[0].observed_at, next.as_str(), total), (9, "4", 10));
    assert_eq!(points_db.all(&all).await.unwrap().len(), 10);
    assert_eq!(
        points_db
            .count(&ListOptions::default().filter("alarm = true"))
            .await
            .unwrap(),
        0
    );
}
