fn main() {
    let proto = "../../proto/libch.proto";
    println!("cargo:rerun-if-changed={proto}");
    prost_build::compile_protos(&[proto], &["../../proto"]).expect("protoc compiles proto/libch.proto");
}
