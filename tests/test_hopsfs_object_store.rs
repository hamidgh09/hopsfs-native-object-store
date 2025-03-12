#[cfg(feature = "hopsfs-integration-test")]
mod test {
    use bytes::Bytes;
    use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
    use object_store::path::Path;
    use hdfs_native_object_store::HdfsObjectStore;
    #[test]
    fn test_hopsfs_connect() -> object_store::Result<()> {
        HdfsObjectStore::with_url("hdfs://127.0.0.1:8020")?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hopsfs_copy_file() -> object_store::Result<()> {
        let store = HdfsObjectStore::with_url("hdfs://127.0.0.1:8020")?;

        store.put_opts(
            &Path::from("/test-copy-file"),
            PutPayload::from(Bytes::from(Bytes::from("some random bytes"))),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        ).await?;
        store.put_opts(
            &Path::from("/test-copy-file2"),
            PutPayload::from_bytes(Bytes::from("some random bytes on the second file")),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        ).await?;

        store.copy(&Path::from("/test-copy-file"), &Path::from("/test-copied-file")).await?;
        store.copy(&Path::from("/test-copy-file2"), &Path::from("/test-copied-file")).await?;
        store.delete(&Path::from("/test-copy-file")).await?;
        store.delete(&Path::from("/test-copy-file2")).await?;
        store.delete(&Path::from("/test-copied-file")).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hopsfs_head() -> object_store::Result<()> {
        let store = HdfsObjectStore::with_url("hdfs://127.0.0.1:8020")?;
        store.put_opts(
            &Path::from("/test-head"),
            PutPayload::new(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        ).await?;
        let metadata = store.head(&Path::from("/test-head")).await?;
        println!("{:?}", metadata);
        store.delete(&Path::from("/test-head")).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_put_with_content() -> object_store::Result<()> {
        let store = HdfsObjectStore::with_url("hdfs://127.0.0.1:8020")?;
        let bytes = Bytes::from("some random bytes");

        store.put_opts(
            &Path::from("/test-put-with-content"),
            PutPayload::from_bytes(bytes),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        ).await?;
        store.delete(&Path::from("/test-put-with-content")).await?;
        Ok(())
    }
}
