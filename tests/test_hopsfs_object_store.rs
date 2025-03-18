#[cfg(feature = "hopsfs-integration-test")]
mod test {
    use bytes::{BufMut, Bytes, BytesMut};
    use hdfs_native_object_store::HdfsObjectStore;
    use object_store::path::Path;
    use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
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
        store
            .put_opts(
                &Path::from("/test-head"),
                PutPayload::new(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
        let metadata = store.head(&Path::from("/test-head")).await?;
        println!("{:?}", metadata);
        store.delete(&Path::from("/test-head")).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_put_with_content() -> object_store::Result<()> {
        let store = HdfsObjectStore::with_url("hdfs://127.0.0.1:8020")?;
        let test_file_ints = 10_000_000;
        let mut buf = BytesMut::new();
        for i in 0..test_file_ints {
            buf.put_i32(i);
        }

        store
            .put_opts(
                &Path::from("/test-put-with-content"),
                PutPayload::from_bytes(buf.freeze()),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;

        store.delete(&Path::from("/test-put-with-content")).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_put() -> object_store::Result<()> {
        let store = HdfsObjectStore::with_url("hdfs://127.0.0.1:8020")?;
        let test_file_ints = 10_000_000;
        let concurrency = 10;

        // Precompute the paths and payloads so they outlive the futures.
        let mut paths = Vec::with_capacity(concurrency);
        let mut payloads = Vec::with_capacity(concurrency);

        for i in 0..concurrency {
            let mut buf = BytesMut::new();
            for j in 0..test_file_ints as i32 {
                buf.put_i32(j);
            }
            paths.push(Path::from(format!("/concurrency-test-put{}", i)));
            payloads.push(buf.freeze());
        }

        // Create a vector of futures without spawning new tasks.
        let put_futures: Vec<_> = paths
            .iter()
            .zip(payloads.into_iter())
            .map(|(path, payload)| {
                store.put_opts(
                    path,
                    PutPayload::from_bytes(payload),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
            })
            .collect();

        // Await all put operations concurrently.
        let results = futures::future::join_all(put_futures).await;
        for result in results {
            result?;
        }

        // Delete the files after all put operations complete.
        for path in &paths {
            store.delete(path).await?;
        }

        Ok(())
    }
}
