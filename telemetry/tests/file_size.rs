//! Keep the shared telemetry crate within the documented source-size limit.
use std::path::Path;
fn check(path: &Path) {
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            check(&path);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let source = std::fs::read_to_string(&path).unwrap();
            assert!(
                source.lines().count() < 200,
                "{} exceeds 199 lines",
                path.display()
            );
            assert!(
                source.starts_with("//!"),
                "{} needs module documentation",
                path.display()
            );
        }
    }
}
#[test]
fn source_and_tests_stay_under_two_hundred_lines() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    check(&root.join("src"));
    check(&root.join("tests"));
}
