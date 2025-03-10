use std::ffi::{CString};

use crate::native::{hdfsConnect, hdfsCopy, hdfsDisconnect, hdfsFS, hdfsFile, hdfsFreeFileInfo, hdfsGetPathInfo, tObjectKind, tSize};
use bytes::Bytes;
use futures::stream::{self, BoxStream, Stream};
use libc::{c_int, c_short, c_ushort, c_void, int32_t, size_t};
use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
};
use std::sync::{Arc, Mutex};
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
/// 2- Implement the missing functions
#[derive(Clone)]
pub struct WriteOptions {
    pub block_size: Option<c_int>,
    pub replication: Option<c_short>,
    pub overwrite: bool,
    pub create_parent: bool,
    pub buffer_size: c_int,
    pub flags: c_int,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            block_size: None,
            replication: None,
            overwrite: false,
            create_parent: true,
            buffer_size: 0,
            flags: 1,
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
#[derive(Clone, Copy)]
pub struct FileWriter {
    pub file: *mut hdfsFile
}


impl FileWriter {
    pub async fn write(&mut self, fs_internal: *const hdfsFS, mut buf: Bytes) -> Result<usize> {
        use crate::native::hdfsWrite;
        unsafe {
            let ptr: *const u8 = buf.as_ptr();
            let res = hdfsWrite(fs_internal, self.file, ptr as *const c_void, buf.len() as tSize);
            if res == -1 {
                Err(HdfsError::OperationFailed("File write operation failed".to_string()))?;
            }
        }
        Ok(buf.len())
    }

    pub async fn close(&mut self, fs_internal: *const hdfsFS) -> Result<()> {
        use crate::native::hdfsCloseFile;
        unsafe {
            let res = hdfsCloseFile(fs_internal, self.file);
            if res == -1 {
                Err(HdfsError::OperationFailed("File close operation failed".to_string()))?;
            }
        }
        Ok(())
    }
}

//TODO: VERY BAD! FIX THIS
unsafe impl Send for FileWriter {}
unsafe impl Sync for FileWriter {}

#[derive(Debug)]
pub struct HopsClient {
    pub hdfs_internal: *mut hdfsFS,
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
        use crate::native::hdfsOpenFile;

        if self.get_file_info(path).await.is_ok(){
            Err(HdfsError::AlreadyExists(path.to_string()))?
        }

        let path = CString::new(path).unwrap();
        unsafe {
            let hdfs_file = hdfsOpenFile(
                self.hdfs_internal, path.as_ptr(),
                opts.flags,
                opts.buffer_size,
                opts.replication.unwrap_or(0),
                opts.block_size.unwrap_or(0) as int32_t
            );
            Ok(FileWriter { file: hdfs_file.cast_mut() })
        }
    }

    pub async fn rename(&self, from: &str, to: &str, _overwrite: bool) -> Result<()> {
        use crate::native::hdfsRename;
        unsafe {
            let _from = CString::new(from).unwrap();
            let _to = CString::new(to).unwrap();
            let res = hdfsRename(self.hdfs_internal, _from.as_ptr(), _to.as_ptr());
            if res != 0 {
                return Err(HdfsError::OperationFailed("rename failed!".to_string()))
            }
        }
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

    pub async fn hdfs_copy(&self, src: &str, dst: &str, overwrite: bool) -> Result<()> {

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

    pub async fn hdfs_write(&self, file_writer: FileWriter, mut buf: Bytes) -> Result<()> {
        use crate::native::hdfsWrite;
        unsafe {
            let ptr: *const u8 = buf.as_ptr();
            let res = hdfsWrite(self.hdfs_internal, file_writer.file, ptr as *const c_void, buf.len() as tSize);
            if res == -1 {
                Err(HdfsError::OperationFailed("File write operation failed".to_string()))?;
            }
        }
        Ok(())
    }

    pub async fn close(&self, file_writer: FileWriter) -> Result<()> {
        use crate::native::hdfsCloseFile;
        unsafe {
            let res = hdfsCloseFile(self.hdfs_internal, file_writer.file);
            if res == -1 {
                Err(HdfsError::OperationFailed("File close operation failed".to_string()))?;
            }
        }
        Ok(())
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
