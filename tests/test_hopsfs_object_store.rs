#[cfg(feature = "integration-test")]
mod test {
    use bytes::{BufMut, Bytes, BytesMut};
    use futures::StreamExt;
    use hdfs_native_object_store::HdfsObjectStore;
    use object_store::path::Path;
    use object_store::{
        GetOptions, GetRange, GetResultPayload, ObjectStore, PutMode, PutOptions, PutPayload,
    };
    use std::collections::HashMap;
    #[test]
    fn test_hopsfs_connect() -> object_store::Result<()> {
        HdfsObjectStore::with_url("hdfs://127.0.0.1:8020")?;
        Ok(())
    }

    #[test]
    fn test_hopsfs_connect_with_config() -> object_store::Result<()> {
        let config = HashMap::new();
        HdfsObjectStore::with_config("hdfs://127.0.0.1:8020", config)?;
        Ok(())
    }
    #[tokio::test]
    async fn test_hopsfs_copy_file() -> object_store::Result<()> {
        let store = HdfsObjectStore::with_url("hdfs://127.0.0.1:8020")?;

        store
            .put_opts(
                &Path::from("/test-copy-file"),
                PutPayload::from(Bytes::from(Bytes::from("some random bytes"))),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
        store
            .put_opts(
                &Path::from("/test-copy-file2"),
                PutPayload::from_bytes(Bytes::from("some random bytes on the second file")),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;

        store
            .copy(
                &Path::from("/test-copy-file"),
                &Path::from("/test-copied-file"),
            )
            .await?;
        store
            .copy(
                &Path::from("/test-copy-file2"),
                &Path::from("/test-copied-file"),
            )
            .await?;
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
    async fn test_put_and_pread() -> object_store::Result<()> {
        let store = HdfsObjectStore::with_url("hdfs://127.0.0.1:8020")?;
        let test_file_size = 65533;
        let mut buf = BytesMut::new();
        for _ in 0..test_file_size {
            buf.put_i8(b'1' as i8);
        }
        let buf_le = buf.len();

        store
            .put_opts(
                &Path::from("/pread_test"),
                PutPayload::from_bytes(buf.freeze()),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await?;

        let get_opts = GetOptions {
            range: Some(GetRange::Suffix(buf_le)),
            ..Default::default()
        };

        let result = store.get_opts(&Path::from("/pread_test"), get_opts).await?;

        let data = match result.payload {
            GetResultPayload::Stream(mut stream) => {
                let mut data = Vec::new();

                // Iterate over each chunk in the stream
                while let Some(chunk_result) = stream.next().await {
                    let chunk = chunk_result?;
                    data.extend_from_slice(&chunk);
                }
                data
            }
            _ => vec![],
        };
        assert_eq!(data.len(), buf_le);

        store.delete(&Path::from("/pread_test")).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_put() -> object_store::Result<()> {
        let store = HdfsObjectStore::with_url("hdfs://127.0.0.1:8020")?;
        let test_file_ints = 8*1024*1024;
        let concurrency = 10;

        // Precompute the paths and payloads so they outlive the futures.
        let mut paths = Vec::with_capacity(concurrency);
        let mut payloads = Vec::with_capacity(concurrency);

        for i in 0..concurrency {
            let mut buf = BytesMut::new();
            for j in 0..test_file_ints {
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
