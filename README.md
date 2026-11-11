# HopsFS Object Store
An [object_store](https://docs.rs/object_store/latest/object_store/) implementation for HopsFS/HDFS using libhdfs Go client bindings.

This is a fork of [hdfs-native-object-store](https://github.com/datafusion-contrib/hdfs-native-object-store) modified to use HopsFS Go client libraries instead of the native Rust hdfs-native library.

# Compatibility
| hopsfs-object-store | Upstream Version | object_store    | HopsFS                             |
|---------------------|------------------|-----------------|------------------------------------|
| 1.2.1               | 0.15.x           | >=0.12.2, <0.13 | >=3.4.3.1-EE-RC0                   |
| 1.1.1               | 0.15.x           | >=0.12.2, <0.13 | >=3.2.0.18-EE-RC1, <3.4.3.1-EE-RC0 |
| 1.1.0               | 0.15.x           | >=0.12.2, <0.13 | 3.2.0.18-EE-RC1                    |
| 1.0.3               | 0.14.x           | 0.12            | 3.2.0.18-SNAPSHOT                  |

# Usage
```rust
use hopsfs_native_object_store::HdfsObjectStoreBuilder;
let store = HdfsObjectStoreBuilder::new()
    .with_url("hdfs://localhost:8020")
    .build()?;
```

# Documentation
See [Documentation](https://docs.rs/hdfs-native-object-store).
