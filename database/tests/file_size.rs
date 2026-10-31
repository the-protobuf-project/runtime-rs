//! Keep the user's source-file size rule executable as the crate grows.
use std::path::Path;

fn check(directory: &Path) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            check(&path);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let text = std::fs::read_to_string(&path).unwrap();
            assert!(
                text.lines().count() < 200,
                "{} must stay under 200 lines including docs",
                path.display()
            );
            assert!(
                text.starts_with("//!"),
                "{} needs module documentation",
                path.display()
            );
        }
    }
}

#[test]
fn all_rust_files_stay_under_200_lines_including_documentation() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for directory in ["src", "tests", "examples"] {
        check(&root.join(directory));
    }
}
