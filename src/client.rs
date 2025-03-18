use libc::{O_RDONLY, O_WRONLY};
use std::ffi::{CStr, CString};

use crate::native::{hdfsCloseFile, hdfsConnect, hdfsCopy, hdfsCreateDirectory, hdfsDelete, hdfsDisconnect, hdfsFS, hdfsFile, hdfsFileInfo, hdfsFreeFileInfo, hdfsGetPathInfo, hdfsPread, hdfsWrite, tObjectKind, tSize};
use bytes::Bytes;
use futures::stream::Stream;
use futures::StreamExt;
use libc::{c_int, c_short, c_ushort, c_void, int32_t};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;
use thiserror::Error;
use tokio::task;

const DATA_BLOCK_SIZE: usize = 65536;
const MAX_CONNECTIONS: usize = 4;

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
            buffer_size: 0,
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
            let path = CStr::from_ptr((*file_info).mName)
                .to_str()
                .ok()
                .map(|s| s.to_owned());

            let owner = CStr::from_ptr((*file_info).mOwner)
                .to_str()
                .ok()
                .map(|s| s.to_owned());

            let group = CStr::from_ptr((*file_info).mGroup)
                .to_str()
                .ok()
                .map(|s| s.to_owned());

            Ok(FileStatus {
                path: path.unwrap(),
                length: (*file_info).mSize as usize,
                isdir: (*file_info).mKind == tObjectKind::kObjectKindDirectory,
                permission: (*file_info).mPermissions as u16,
                owner: owner.unwrap(),
                group: group.unwrap(),
                modification_time: (*file_info).mLastMod as u64,
                access_time: (*file_info).mLastAccess as u64,
                replication: Some((*file_info).mReplication as u32),
                blocksize: Some((*file_info).mBlockSize as u64),
            })
        }
    }
}

pub struct FileReader {
    pub file: Arc<AtomicPtr<hdfsFile>>,
    path: String,
}

impl FileReader {
    pub fn new(file: *mut hdfsFile, path: String) -> Result<Self> {
        Ok(FileReader {
            file: Arc::new(AtomicPtr::new(file)),
            path,
        })
    }
    pub fn get_file_ptr(&self) -> *const hdfsFile {
        self.file.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct FileWriter {
    file: Arc<AtomicPtr<hdfsFile>>,
}

impl FileWriter {
    pub fn new(file: *mut hdfsFile) -> Result<Self> {
        Ok(FileWriter {
            file: Arc::new(AtomicPtr::new(file)),
        })
    }
    pub fn get_file_ptr(&self) -> *const hdfsFile {
        self.file.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct Connection {
    pub ptr: AtomicPtr<hdfsFS>,
}

impl Connection {
    pub fn new(ptr: *const hdfsFS) -> Self {
        Connection {
            ptr: AtomicPtr::new(ptr.cast_mut()),
        }
    }

    fn get_conn_ptr(&self) -> *const hdfsFS {
        self.ptr.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct HopsClient {
    pub hdfs_internal: Arc<Vec<Arc<Connection>>>,
    next_conn_idx: AtomicUsize,
}

impl Drop for HopsClient {
    /// Disconnect from the HDFS filesystem.
    /// This can potentially cause problem if the disconnect fails.
    /// Yet there is no explicit close process exists in the lib.rs
    fn drop(&mut self) {
        for i in 0..MAX_CONNECTIONS {
            unsafe {
                let ret = hdfsDisconnect(self.hdfs_internal[i].get_conn_ptr());
                if ret != 0 {
                    eprintln!("hdfsDisconnect failed with error code: {}", ret);
                }
            }
        }
    }
}

impl HopsClient {
    pub fn new(url: &str) -> Result<Self> {
        let mut connections = Vec::with_capacity(MAX_CONNECTIONS);
        for _ in 0..MAX_CONNECTIONS {
            let fs = Self::hopsfs_connect_with_url(url)?;
            let connection = Arc::new(Connection::new(fs));
            connections.push(connection);
        }
        Ok(HopsClient {
            hdfs_internal: Arc::new(connections),
            next_conn_idx: AtomicUsize::new(0),
        })
    }

    pub fn get_connection(&self) -> Arc<Connection> {
        let curr_index = loop {
            let current = self.next_conn_idx.load(Ordering::SeqCst);
            let new = (current + 1) % MAX_CONNECTIONS;
            match self.next_conn_idx.compare_exchange(
                current,
                new,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(prev) => break prev,
                Err(_) => continue,
            }
        };
        Arc::clone(&self.hdfs_internal[curr_index])
    }

    pub fn hopsfs_connect_with_url(uri: &str) -> Result<*const hdfsFS> {
        let (host_str, port_u16) = extract_host_and_port(uri);

        let c_host = CString::new(host_str).expect("CString conversion failed");
        let c_port: c_ushort = port_u16;

        let max_retries = 3;
        let mut attempt = 0;
        while attempt < max_retries {
            unsafe {
                let fs = hdfsConnect(c_host.as_ptr(), c_port);
                if !fs.is_null() {
                    return Ok(fs);
                }
            }
            attempt += 1;
            thread::sleep(Duration::from_millis(500));
        }

        Err(HdfsError::OperationFailed(format!(
            "Connection to HopsFS failed after {} attempts! {}",
            max_retries,
            uri.to_string()
        )))
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
            let connection = self.get_connection();

            // TODO: Fix this with manual thread.
            // Note that Send is not implemented for pointers!
            let path_info = hdfsGetPathInfo(connection.get_conn_ptr(), refined_path.as_ptr());

            if path_info.is_null() {
                return Err(HdfsError::FileNotFound(path.to_string()));
            }

            let owner = CStr::from_ptr((*path_info).mOwner)
                .to_str()
                .ok()
                .map(|s| s.to_owned());

            let group = CStr::from_ptr((*path_info).mGroup)
                .to_str()
                .ok()
                .map(|s| s.to_owned());

            let file_status = FileStatus {
                path: path.to_string(),
                length: (*path_info).mSize as usize,
                isdir: (*path_info).mKind == tObjectKind::kObjectKindDirectory,
                permission: (*path_info).mPermissions as u16,
                owner: owner.unwrap(),
                group: group.unwrap(),
                modification_time: (*path_info).mLastMod as u64,
                access_time: (*path_info).mLastAccess as u64,
                replication: Some((*path_info).mReplication as u32),
                blocksize: Some((*path_info).mBlockSize as u64),
            };

            hdfsFreeFileInfo(path_info, 1);
            Ok(file_status)
        }
    }

    pub async fn open_for_read(&self, path: &str) -> Result<FileReader> {
        use crate::native::hdfsOpenFile;

        if self.get_file_info(path).await.is_err() {
            Err(HdfsError::InvalidPath(path.to_string()))?
        }
        let path_cstr = CString::new(path).unwrap();
        let connection = self.get_connection();
        // TODO: Fix this with manual thread.
        // Note that Send is not implemented for pointers!
        unsafe {
            let hdfs_file = hdfsOpenFile(
                connection.get_conn_ptr(),
                path_cstr.as_ptr(),
                O_RDONLY,
                0,
                0,
                0,
            );
            FileReader::new(hdfs_file.cast_mut(), path.to_string())
        }
    }

    pub async fn create(&self, path: &str, opts: WriteOptions) -> Result<FileWriter> {
        use crate::native::hdfsOpenFile;

        if self.get_file_info(path).await.is_ok() {
            if !opts.overwrite {
                Err(HdfsError::AlreadyExists(path.to_string()))?
            }
        }
        let path = CString::new(path).unwrap();
        let connection = self.get_connection();
        // TODO: Fix this with manual thread.
        // Note that Send is not implemented for pointers!
        unsafe {
            let hdfs_file = hdfsOpenFile(
                connection.get_conn_ptr(),
                path.as_ptr(),
                O_WRONLY,
                opts.buffer_size,
                opts.replication.unwrap_or(0),
                opts.block_size.unwrap_or(0) as int32_t,
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
            let connection = self.get_connection();

            let res = task::spawn_blocking(move || unsafe {
                hdfsRename(connection.get_conn_ptr(), _from.as_ptr(), _to.as_ptr())
            }).await;

            if res.is_err() || res.unwrap() != 0 {
                return Err(HdfsError::OperationFailed("rename failed!".to_string()));
            }
        }
        Ok(())
    }

    pub async fn delete(&self, path: &str, _recursive: bool) -> Result<bool> {
        use crate::native::hdfsDelete;
        let _path = CString::new(path).unwrap();
        let connection = self.get_connection();

        let res = task::spawn_blocking(move || unsafe {
            hdfsDelete(
                connection.get_conn_ptr(),
                _path.as_ptr(),
                _recursive as c_int,
            )
        }).await;

        if res.is_err() || res.unwrap() == -1 {
            return Err(HdfsError::OperationFailed("Failed to delete file".to_string()))
        }

        Ok(true)
    }

    pub async fn list_directory(&self, prefix: &str) -> Result<Vec<FileStatus>> {
        use crate::native::hdfsListDirectory;
        let path_cstr =
            CString::new(prefix).map_err(|_| HdfsError::InvalidPath(prefix.to_string()))?;

        let mut num_entries: c_int = 0;
        let connection = self.get_connection();
        unsafe {
            let response = hdfsListDirectory(
                connection.get_conn_ptr(),
                path_cstr.as_ptr(),
                &mut num_entries,
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
        let connection = self.get_connection();

        let res = task::spawn_blocking(move || unsafe {
            let conn_ptr_usize = connection.get_conn_ptr();
            hdfsCopy(
                conn_ptr_usize,
                src_cstr.as_ptr(),
                conn_ptr_usize,
                dst_cstr.as_ptr(),
            )
        })
        .await;

        if res.is_ok() || res.unwrap() == 0 {
            Ok(())
        } else {
            Err(HdfsError::OperationFailed("Failed to copy file".to_string()))
        }
    }

    pub async fn hdfs_write(&self, file_writer: &FileWriter, buf: Bytes) -> Result<()> {
        use crate::native::hdfsWrite;

        let connection = self.get_connection();
        let file_ptr = file_writer.get_file_ptr() as usize;

        let res = task::spawn_blocking(move || {
            let buf_ptr = buf.as_ptr().cast::<c_void>();
            let buf_len = buf.len() as tSize;
            unsafe {
                hdfsWrite(
                    connection.get_conn_ptr(),
                    file_ptr as *const hdfsFile,
                    buf_ptr,
                    buf_len,
                )
            }
        })
        .await;

        if res.is_err() || res.unwrap() == -1 {
            Err(HdfsError::OperationFailed("File write operation failed".to_string()))
        } else {
            Ok(())
        }
    }

    pub async fn close_file(&self, file_writer: FileWriter) -> Result<()> {
        use crate::native::hdfsCloseFile;
        let connection = self.get_connection();
        let res = task::spawn_blocking(move || unsafe {
            hdfsCloseFile(connection.get_conn_ptr(), file_writer.get_file_ptr())
        })
        .await;

        if res.is_err() || res.unwrap() == -1 {
            return Err(HdfsError::OperationFailed("File close operation failed".to_string()));
        }
        Ok(())
    }

    pub async fn mkdir(&self, path: &str) -> Result<()> {
        use crate::native::hdfsCreateDirectory;
        let path_cstr = CString::new(path).unwrap();
        let connection = self.get_connection();

        let res = task::spawn_blocking(move || unsafe {
            hdfsCreateDirectory(connection.get_conn_ptr(), path_cstr.as_ptr())
        })
        .await;

        if res.is_err() || res.unwrap() == -1 {
            return Err(HdfsError::OperationFailed("Failed to create directory".to_string()));
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

pub struct ReadRangeStream {
    connection: Arc<Connection>,
    file_pointer: Arc<AtomicPtr<hdfsFile>>,
    current: usize,
    end: usize,
}

impl ReadRangeStream {
    pub fn new(
        connection: Arc<Connection>,
        file_pointer: Arc<AtomicPtr<hdfsFile>>,
        start: usize,
        end: usize,
    ) -> Self {
        Self {
            connection,
            file_pointer,
            current: start,
            end,
        }
    }
}

impl Stream for ReadRangeStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.current >= self.end {
            return Poll::Ready(None);
        }

        let current_end = std::cmp::min(self.current + DATA_BLOCK_SIZE, self.end);
        let length = current_end - self.current;
        let mut buffer = vec![0u8; length];

        unsafe {
            let result = hdfsPread(
                self.connection.get_conn_ptr(),
                self.file_pointer.load(Ordering::SeqCst),
                self.current as i64,
                buffer.as_mut_ptr() as *mut c_void,
                length as tSize,
            );
            if result <= 0 || result as usize != length {
                return Poll::Ready(Some(Err(HdfsError::OperationFailed(
                    "Failed to read file".into(),
                ))));
            }
            self.current = current_end;
            let result_bytes = Bytes::copy_from_slice(&buffer[..result as usize]);
            Poll::Ready(Some(Ok(result_bytes)))
        }
    }
}

impl Drop for ReadRangeStream {
    fn drop(&mut self) {
        unsafe {
            hdfsCloseFile(
                self.connection.get_conn_ptr(),
                self.file_pointer.load(Ordering::SeqCst),
            );
        }
    }
}
