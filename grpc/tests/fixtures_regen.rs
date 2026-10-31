//! Regenerates the shop fixture the error/validation tests compile against.
//!
//! The generated code and descriptor set are checked in, so building the tests needs no
//! `protoc`; this is the one place that does. Run it after editing `tests/proto`:
//!
//! ```text
//! cargo test -p tpp-grpc --test fixtures_regen -- --ignored
//! ```

use std::path::PathBuf;

#[test]
#[ignore = "needs protoc; run by hand after editing tests/proto"]
fn regenerate_shop_fixture() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");
    let protos = root.join("proto");
    let fixtures = root.join("fixtures");
    tonic_prost_build::configure()
        .build_transport(false)
        .emit_rerun_if_changed(false)
        .out_dir(&fixtures)
        .file_descriptor_set_path(fixtures.join("shop.binpb"))
        .compile_protos(&[protos.join("shop/v1/shop.proto")], &[protos])
        .expect("the shop protos compile");
    // The imports are read for their options only; their generated types are never used.
    for unused in ["buf.validate.rs", "google.api.rs"] {
        let _ = std::fs::remove_file(fixtures.join(unused));
    }
}
