//! `libch::proto` compiles in the code `prost-build` generates from `proto/libch.proto`, so a build of the crate
//! needs no `protoc`. This test generates the code again and compares. After a change to the protocol, run it
//! with `LIBCH_PROTO_WRITE=1` to write the new code in place.

use std::fs;

use libch_testkit::Stack;

#[test]
fn the_crates_protocol_code_is_what_the_proto_generates() {
    let root = Stack::repo_root();
    let out = std::env::temp_dir().join(format!("libch-proto-{}", std::process::id()));
    fs::create_dir_all(&out).unwrap();
    prost_build::Config::new()
        .out_dir(&out)
        .compile_protos(&[root.join("proto/libch.proto")], &[root.join("proto")])
        .expect("protoc compiles proto/libch.proto");
    let generated = fs::read_to_string(out.join("libch.v1.rs")).unwrap();
    fs::remove_dir_all(&out).unwrap();

    let kept = root.join("crates/libch/src/proto/libch.v1.rs");
    if std::env::var_os("LIBCH_PROTO_WRITE").is_some() {
        fs::write(&kept, &generated).unwrap();
    }
    assert!(
        fs::read_to_string(&kept).unwrap() == generated,
        "{} differs from what proto/libch.proto generates: run `LIBCH_PROTO_WRITE=1 cargo test -p libch-testkit --test proto`",
        kept.display()
    );
}
