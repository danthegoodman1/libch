//! The S3 client against the dev stack's service.

use bytes::Bytes;
use libch::s3::{Object, S3Client};
use libch_testkit::stack;

#[tokio::test]
async fn objects_round_trip() {
    let stack = stack().await;
    let prefix = format!("s3-test-{:016x}", rand::random::<u64>());
    let s3 = S3Client::new(&stack.s3(&prefix)).unwrap();
    let key = |name: &str| format!("{prefix}/{name}");

    assert_eq!(s3.size(&key("absent")).await.unwrap(), None);
    assert_eq!(s3.get(&key("absent")).await.unwrap(), None);
    assert!(s3.list(&format!("{prefix}/")).await.unwrap().is_empty());
    s3.delete(&key("absent")).await.unwrap();

    s3.put(&key("a/one"), Bytes::from_static(b"first")).await.unwrap();
    s3.put(&key("a/two"), Bytes::from(vec![7u8; 100_000])).await.unwrap();
    s3.put(&key("b"), Bytes::new()).await.unwrap();
    assert_eq!(s3.size(&key("a/one")).await.unwrap(), Some(5));
    assert_eq!(s3.size(&key("a/two")).await.unwrap(), Some(100_000));
    assert_eq!(s3.size(&key("b")).await.unwrap(), Some(0));
    assert_eq!(s3.get(&key("a/one")).await.unwrap().as_deref(), Some(&b"first"[..]));

    let mut listed = s3.list(&format!("{prefix}/")).await.unwrap();
    listed.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(listed, [Object { key: key("a/one"), size: 5 }, Object { key: key("a/two"), size: 100_000 }, Object { key: key("b"), size: 0 }]);
    assert_eq!(s3.list(&key("a/")).await.unwrap().len(), 2);

    s3.put(&key("a/one"), Bytes::from_static(b"replaced")).await.unwrap();
    assert_eq!(s3.size(&key("a/one")).await.unwrap(), Some(8));

    s3.delete(&key("b")).await.unwrap();
    assert_eq!(s3.size(&key("b")).await.unwrap(), None);
    assert_eq!(s3.delete_prefix(&format!("{prefix}/")).await.unwrap(), 2);
    assert!(s3.list(&format!("{prefix}/")).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_long_listing_follows_its_pages() {
    let stack = stack().await;
    let prefix = format!("s3-test-{:016x}", rand::random::<u64>());
    let s3 = S3Client::new(&stack.s3(&prefix)).unwrap();
    // More objects than one page of a listing holds.
    for chunk in (0..1_100).collect::<Vec<u32>>().chunks(64) {
        let puts = chunk.iter().map(|index| {
            let key = format!("{prefix}/{index:05}");
            let s3 = &s3;
            async move { s3.put(&key, Bytes::from_static(b"x")).await }
        });
        futures_util::future::try_join_all(puts).await.unwrap();
    }
    let listed = s3.list(&format!("{prefix}/")).await.unwrap();
    assert_eq!(listed.len(), 1_100);
    assert_eq!(s3.delete_prefix(&format!("{prefix}/")).await.unwrap(), 1_100);
}
