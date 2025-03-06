#[cfg(feature = "hopsfs-integration-test")]
mod test {
    use hdfs_native_object_store::HdfsObjectStore;
    #[test]
    fn test_hopsfs_connect() -> object_store::Result<()> {
        let store = HdfsObjectStore::with_url("hdfs://127.0.0.1:9000")?;
        Ok(())
    }
}
