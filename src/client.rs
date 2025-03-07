use std::ffi::CString;

use crate::native::{hdfsConnect, hdfsCopy, hdfsDisconnect, hdfsFS, hdfsFreeFileInfo, hdfsGetPathInfo, tObjectKind};
use bytes::Bytes;
use futures::stream::{self, BoxStream, Stream};
use libc::c_ushort;
use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HdfsError {
    #[error("File not found at {0}")]
    FileNotFound(String),
    #[error("File already exists at {0}")]
    AlreadyExists(String),
    #[error("Invalid path to {0}")]
    InvalidPath(String),
    #[error("{0} is a directory")]
    IsADirectoryError(String),
    #[error("Operation failed: {0}")]
    OperationFailed(String),
    #[error("Invalid connection URL {0}")]
    InvalidUri(String),
    #[error("{0} is not supported yet!")]
    NotSupported(String),
}

pub type Result<T> = std::result::Result<T, HdfsError>;

/// Derived from HDFS Client
/// TODO:
/// 1- Make it compatible with HopsFS
/// 2- Implement the missing functions
#[derive(Clone)]
pub struct WriteOptions {
    /// Block size. Default is retrieved from the server.
    pub block_size: Option<u64>,
    /// Replication factor. Default is retrieved from the server.
    pub replication: Option<u32>,
    /// Unix file permission, defaults to 0o644, which is "rw-r--r--" as a Unix permission.
    /// This is the raw octal value represented in base 10.
    pub permission: u32,
    /// Whether to overwrite the file, defaults to false. If true and the
    /// file does not exist, it will result in an error.
    pub overwrite: bool,
    /// Whether to create any missing parent directories, defaults to true. If false
    /// and the parent directory does not exist, an error will be returned.
    pub create_parent: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            block_size: None,
            replication: None,
            permission: 0o644,
            overwrite: false,
            create_parent: true,
        }
    }
}

/// Derived from HDSF Client
/// TODO: Make sure it's compatible with HopsFS
#[derive(Debug)]
pub struct FileStatus {
    pub path: String,
    pub length: usize,
    pub isdir: bool,
    pub permission: u16,
    pub owner: String,
    pub group: String,
    pub modification_time: u64,
    pub access_time: u64,
    pub replication: Option<u32>,
    pub blocksize: Option<u64>,
}

/// TODO: Implement this
pub struct FileStatusIter {
    pub inner: Pin<Box<dyn Stream<Item = Result<FileStatus>> + Send>>,
}

impl FileStatusIter {
    pub fn into_stream(self) -> BoxStream<'static, Result<FileStatus>> {
        self.inner
    }
}

impl Stream for FileStatusIter {
    type Item = Result<FileStatus>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// TODO: Implement FileReader
pub struct FileReader {}

impl FileReader {
    pub fn new() -> Self {
        FileReader {}
    }

    pub fn file_length(&self) -> usize {
        unimplemented!("File length not implemented");
    }

    pub fn read_range_stream(
        &self,
        _offset: usize,
        _length: usize,
    ) -> impl Stream<Item = Result<Bytes>> {
        unimplemented!("Read range stream not implemented");
        Box::pin(stream::empty())
    }
}

/// TODO: Implement FileWriter
pub struct FileWriter {}

impl FileWriter {
    pub fn new() -> Self {
        FileWriter {}
    }

    pub async fn write(&mut self, mut buf: Bytes) -> Result<usize> {
        unimplemented!("Write operation not implemented");
        Ok(buf.len())
    }

    pub async fn close(&mut self) -> Result<()> {
        unimplemented!("Close operation not implemented");
        Ok(())
    }
}

#[derive(Debug)]
pub struct HopsClient {
    hdfs_internal: *mut hdfsFS,
}

///TODO: Make sure the unsafe impls are safe
unsafe impl Sync for HopsClient {}
unsafe impl Send for HopsClient {}

impl Drop for HopsClient {
    /// Disconnect from the HDFS filesystem.
    /// This can potentially cause problem if the disconnect fails.
    /// Yet there is no explicit close function exists in the lib.rs
    /// TODO:
    /// - Make sure we handle connection close process properly
    fn drop(&mut self) {
        unsafe {
            let ret = hdfsDisconnect(self.hdfs_internal);
            if ret != 0 {
                eprintln!("hdfsDisconnect failed with error code: {}", ret);
            }
            self.hdfs_internal = std::ptr::null_mut();
        }
    }
}
impl HopsClient {
    pub fn new(url: &str) -> Result<Self> {
        let fs = Self::hopsfs_connect_with_url(url)?;
        Ok(HopsClient {
            hdfs_internal: fs.cast_mut(),
        })
    }
    pub fn hopsfs_connect_with_url(uri: &str) -> Result<*const hdfsFS> {
        let (host_str, port_u16) = extract_host_and_port(uri);

        let c_host = CString::new(host_str).expect("CString conversion failed");
        let c_port: c_ushort = port_u16;

        unsafe {
            let fs = hdfsConnect(c_host.as_ptr(), c_port);
            if fs.is_null() {
                Err(HdfsError::OperationFailed(format!(
                    "Connection to HopsFS failed! {}",
                    uri.to_string()
                )))
            } else {
                Ok(fs)
            }
        }
    }

    /// TODO: Check if we should support this!
    pub fn new_with_config(_url: &str, _config: HashMap<String, String>) -> Result<Self> {
        eprintln!("Creating new connection with configuration is not supported yet!");
        eprintln!("Using default connection instead!");
        Self::new(_url)
    }

    pub async fn get_file_info(&self, path: &str) -> Result<FileStatus> {
        unsafe {
            let refined_path = CString::new(path).unwrap();
            let path_info = hdfsGetPathInfo(self.hdfs_internal, refined_path.as_ptr());

            if path_info.is_null() {
                return Err(HdfsError::FileNotFound(path.to_string()));
            }

            // TODO: Fix the owner and group fields
            let file_status = FileStatus {
                path : path.to_string(),
                length : (*path_info).mSize as usize,
                isdir : (*path_info).mKind == tObjectKind::kObjectKindDirectory,
                permission : (*path_info).mPermissions as u16,
                owner : "".to_string(),
                group : "".to_string(),
                modification_time : (*path_info).mLastMod as u64,
                access_time : (*path_info).mLastAccess as u64,
                replication : Some((*path_info).mReplication as u32),
                blocksize : Some((*path_info).mBlockSize as u64),
            };

            hdfsFreeFileInfo(path_info, 1);
            Ok(file_status)
        }
    }

    pub async fn read(&self, _path: &str) -> Result<FileReader> {
        unimplemented!("Read not implemented");
        Ok(FileReader::new())
    }

    pub async fn create(&self, path: &str, opts: WriteOptions) -> Result<FileWriter> {
        unimplemented!();
        Ok(FileWriter::new())
    }

    pub async fn rename(&self, _from: &str, _to: &str, _overwrite: bool) -> Result<()> {
        unimplemented!("Rename not implemented");
        Ok(())
    }

    pub async fn delete(&self, _path: &str, _recursive: bool) -> Result<bool> {
        unimplemented!("Delete not implemented");
        Ok(true)
    }

    pub fn list_status_iter(&self, _prefix: &str, _recursive: bool) -> FileStatusIter {
        unimplemented!("List status iter not implemented");
        FileStatusIter {
            inner: Box::pin(stream::empty()),
        }
    }

    pub fn hdfs_copy(&self, src: &str, dst: &str, overwrite: bool) -> Result<()> {
        ///TODO: Check how overwrite should be handled!
        let src = CString::new(src).unwrap();
        let dst = CString::new(dst).unwrap();
        let res = unsafe {
            hdfsCopy(
                self.hdfs_internal,
                src.as_ptr(),
                self.hdfs_internal,
                dst.as_ptr(),
            )
        };
        if res == 0 {
            Ok(())
        } else {
            Err(HdfsError::OperationFailed(
                "Failed to copy file".to_string(),
            ))
        }
    }
}

fn extract_host_and_port(uri: &str) -> (String, u16) {
    let default_port = 8020;

    let stripped_uri = uri
        .strip_prefix("hdfs://")
        .or_else(|| uri.strip_prefix("hopsfs://"))
        .unwrap_or(uri);

    let mut parts = stripped_uri.split(':');
    let host = parts.next().unwrap_or("").to_string();
    let port = parts
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(default_port);

    (host, port)
}
