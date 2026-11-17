#[cfg(feature = "integration-test")]
mod test {
    use bytes::{BufMut, Bytes, BytesMut};
    use futures::StreamExt;
    use hopsfs_native_object_store::HdfsObjectStore;
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
            range: Some(GetRange::Suffix(buf_le as u64)),
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

    /// Several writers on separate connections put the same path with PutMode::Create at
    /// once, as Delta writers committing the same log version do. Exactly one must
    /// succeed, the others must get AlreadyExists, the file must hold the winner's bytes
    /// and no temp file may be left behind.
    #[tokio::test]
    async fn test_put_create_race() -> object_store::Result<()> {
        let (writers, rounds) = (8, 10);
        let stores = (0..writers)
            .map(|_| HdfsObjectStore::with_url("hdfs://127.0.0.1:8020"))
            .collect::<object_store::Result<Vec<_>>>()?;
        // A fresh directory per run, so a failed run cannot break the next one.
        let run_dir = format!("test-put-create-race-{}", uuid::Uuid::new_v4());

        for round in 0..rounds {
            let parent = Path::from(format!("{run_dir}/round-{round}"));
            let path = parent.child("00000000000000000000.json");
            let puts = stores.iter().enumerate().map(|(i, store)| {
                store.put_opts(
                    &path,
                    PutPayload::from(format!("writer-{i}")),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
            });
            let results = futures::future::join_all(puts).await;

            let winners: Vec<usize> = (0..writers).filter(|i| results[*i].is_ok()).collect();
            assert_eq!(winners.len(), 1, "round {round}: winners {winners:?}");
            for result in &results {
                if let Err(e) = result {
                    assert!(
                        matches!(e, object_store::Error::AlreadyExists { .. }),
                        "round {round}: a losing put failed with {e}"
                    );
                }
            }

            let got = stores[0].get(&path).await?.bytes().await?;
            assert_eq!(got, Bytes::from(format!("writer-{}", winners[0])));

            let listed = stores[0].list_with_delimiter(Some(&parent)).await?;
            assert_eq!(
                listed.objects.len(),
                1,
                "round {round}: leftover files {:?}",
                listed.objects
            );

            // Deletes are not recursive: remove the file, then its directory.
            stores[0].delete(&path).await?;
            stores[0].delete(&parent).await?;
        }

        stores[0].delete(&Path::from(run_dir)).await?;
        Ok(())
    }
}
