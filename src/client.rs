use libc::O_WRONLY;
use std::ffi::{CStr, CString};

use crate::native::{hdfsConnect, hdfsCopy, hdfsDisconnect, hdfsFS, hdfsFile, hdfsFileInfo, hdfsFreeFileInfo, hdfsGetPathInfo, tObjectKind, tSize};
use bytes::Bytes;
use futures::stream::{self, Stream};
use libc::{c_int, c_short, c_ushort, c_void, int32_t};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::collections::HashMap;
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

#[derive(Clone)]
pub struct WriteOptions {
    pub block_size: Option<c_int>,
    pub replication: Option<c_short>,
    pub overwrite: bool,
    pub create_parent: bool, //create_parent false is not supported, yet!
    pub buffer_size: c_int,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            block_size: None,
            replication: None,
            overwrite: false,
            create_parent: true,
            buffer_size: 0
        }
    }
}

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

impl FileStatus {
    pub fn from_hdfs_file_info(file_info: *const hdfsFileInfo) -> Result<Self> {
        unsafe {
            let path = CStr::from_ptr((*file_info).mName).to_str().ok()
                .map(|s| s.to_owned());

            let owner = CStr::from_ptr((*file_info).mOwner).to_str().ok()
                .map(|s| s.to_owned());

            let group = CStr::from_ptr((*file_info).mGroup).to_str().ok()
                .map(|s| s.to_owned());

            Ok(FileStatus {
                path : path.unwrap(),
                length : (*file_info).mSize as usize,
                isdir : (*file_info).mKind == tObjectKind::kObjectKindDirectory,
                permission : (*file_info).mPermissions as u16,
                owner : owner.unwrap(),
                group : group.unwrap(),
                modification_time : (*file_info).mLastMod as u64,
                access_time : (*file_info).mLastAccess as u64,
                replication : Some((*file_info).mReplication as u32),
                blocksize : Some((*file_info).mBlockSize as u64),
            })
        }
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

#[derive(Debug)]
pub struct FileWriter {
    file: Arc<AtomicPtr<hdfsFile>>,
}

impl FileWriter {
    pub fn new(file: *mut hdfsFile) -> Result<Self> {
        Ok(FileWriter {
            file: Arc::new(AtomicPtr::new(file))
        })
    }
    pub fn get_file_ptr(&self) -> *const hdfsFile {
        self.file.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct HopsClient {
    pub hdfs_internal: Arc<AtomicPtr<hdfsFS>>
}

impl Drop for HopsClient {
    /// Disconnect from the HDFS filesystem.
    /// This can potentially cause problem if the disconnect fails.
    /// Yet there is no explicit close process exists in the lib.rs
    fn drop(&mut self) {
        unsafe {
            let ret = hdfsDisconnect(self.get_client_ptr());
            if ret != 0 {
                eprintln!("hdfsDisconnect failed with error code: {}", ret);
            }
            self.hdfs_internal = Arc::new(AtomicPtr::default());
        }
    }
}
impl HopsClient {
    pub fn new(url: &str) -> Result<Self> {
        let fs = Self::hopsfs_connect_with_url(url)?;
        Ok(HopsClient {
            hdfs_internal: Arc::new(AtomicPtr::new(fs.cast_mut())),
        })
    }

    fn get_client_ptr(&self) -> *const hdfsFS {
        self.hdfs_internal.load(Ordering::SeqCst)
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
            let path_info = hdfsGetPathInfo(self.get_client_ptr(), refined_path.as_ptr());

            if path_info.is_null() {
                return Err(HdfsError::FileNotFound(path.to_string()));
            }

            let owner = CStr::from_ptr((*path_info).mOwner).to_str().ok()
                .map(|s| s.to_owned());

            let group = CStr::from_ptr((*path_info).mGroup).to_str().ok()
                .map(|s| s.to_owned());

            let file_status = FileStatus {
                path : path.to_string(),
                length : (*path_info).mSize as usize,
                isdir : (*path_info).mKind == tObjectKind::kObjectKindDirectory,
                permission : (*path_info).mPermissions as u16,
                owner : owner.unwrap(),
                group : group.unwrap(),
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

        if self.get_file_info(path).await.is_ok() {
            if !opts.overwrite {
                Err(HdfsError::AlreadyExists(path.to_string()))?
            }
        }
        let path = CString::new(path).unwrap();
        unsafe {
            let hdfs_file = hdfsOpenFile(
                self.get_client_ptr(), path.as_ptr(),
                O_WRONLY,
                opts.buffer_size,
                opts.replication.unwrap_or(0),
                opts.block_size.unwrap_or(0) as int32_t
            );

            FileWriter::new(hdfs_file.cast_mut())
        }
    }

    pub async fn rename(&self, from: &str, to: &str, overwrite: bool) -> Result<()> {
        use crate::native::hdfsRename;
        unsafe {
            if self.get_file_info(to).await.is_ok() {
                if !overwrite {
                    Err(HdfsError::AlreadyExists(to.to_string()))?
                }
            }

            let _from = CString::new(from).unwrap();
            let _to = CString::new(to).unwrap();

            let res = hdfsRename(self.get_client_ptr(), _from.as_ptr(), _to.as_ptr());
            if res != 0 {
                return Err(HdfsError::OperationFailed("rename failed!".to_string()))
            }
        }
        Ok(())
    }

    pub async fn delete(&self, path: &str, _recursive: bool) -> Result<bool> {
        use crate::native::hdfsDelete;
        let _path = CString::new(path).unwrap();
        unsafe {
            let res = hdfsDelete(self.get_client_ptr(), _path.as_ptr(), _recursive as c_int);
            if res != 0{
                return Ok(false)
            }
        };
        Ok(true)
    }

    pub async fn list_directory(&self, prefix: &str) -> Result<Vec<FileStatus>> {
        use crate::native::hdfsListDirectory;
        let path_cstr = CString::new(prefix).unwrap_or(
            Err(HdfsError::InvalidPath(prefix.to_string()))?
        );

        let mut num_entries: c_int = 0;
        unsafe {
            let response = hdfsListDirectory(
                self.get_client_ptr(),
                path_cstr.as_ptr(),
                &mut num_entries
            );

            if response.is_null() || num_entries == 0 {
                return Ok(vec![]);
            }

            let file_infos = std::slice::from_raw_parts(response, num_entries as usize);
            let mut objects = Vec::with_capacity(num_entries as usize);

            for info in file_infos.iter() {
                match FileStatus::from_hdfs_file_info(info) {
                    Ok(file_info) => objects.push(file_info),
                    Err(e) => {
                        // TODO: Should we stop here or simply ignore the file?
                        eprintln!("Error processing file: {:?}", e);
                    }
                }
            }
            hdfsFreeFileInfo(response, num_entries);
            Ok(objects)
        }
    }

    pub async fn hdfs_copy(&self, src: &str, dst: &str, overwrite: bool) -> Result<()> {

        if self.get_file_info(dst).await.is_ok() {
            if !overwrite {
                Err(HdfsError::AlreadyExists(dst.to_string()))?
            }
        }

        let src_cstr = CString::new(src).unwrap();
        let dst_cstr = CString::new(dst).unwrap();
        let res = unsafe {
            hdfsCopy(
                self.get_client_ptr(),
                src_cstr.as_ptr(),
                self.get_client_ptr(),
                dst_cstr.as_ptr(),
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

    pub async fn hdfs_write(&self, file_writer: &FileWriter, buf: Bytes) -> Result<()> {
        use crate::native::hdfsWrite;
        unsafe {
            let res = hdfsWrite(
                self.get_client_ptr(),
                file_writer.get_file_ptr(),
                buf.as_ptr().cast::<c_void>(),
                buf.len() as tSize
            );
            if res == -1 {
                return Err(HdfsError::OperationFailed("File write operation failed".to_string()));
            }
        }
        Ok(())
    }

    pub async fn close_file(&self, file_writer: FileWriter) -> Result<()> {
        use crate::native::hdfsCloseFile;
        unsafe {
            let res = hdfsCloseFile(self.get_client_ptr(), file_writer.get_file_ptr());
            if res == -1 {
                return Err(HdfsError::OperationFailed("File close operation failed".to_string()));
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
