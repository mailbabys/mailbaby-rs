fn main() {
    println!("cargo:rerun-if-changed=proto/mailbaby.proto");

    #[cfg(feature = "grpc")]
    {
        tonic_prost_build::configure()
            .build_server(false)
            .compile_protos(&["proto/mailbaby.proto"], &["proto"])
            .expect("failed to compile proto/mailbaby.proto with tonic-prost-build");
    }
}
