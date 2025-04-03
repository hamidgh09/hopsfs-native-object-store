use fs_extra::dir::CopyOptions;
use log::info;
use std::env;
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use url::Url;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let lib_url = env::var("HOPS_LIB_URL").unwrap_or_default();
    let lib_dir = Path::new("lib");
    if !lib_dir.exists() {
        fs::create_dir(lib_dir)?;
    }

    if !lib_url.is_empty() {
        extract_tarball(lib_url, lib_dir)?;
    }

    set_libraries();
    Ok(())
}

fn extract_tarball(url: String, lib_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let parsed_url = Url::parse(&url)?;
    let filename = parsed_url
        .path_segments()
        .and_then(|segments| segments.last())
        .and_then(|name| if name.is_empty() { None } else { Some(name) })
        .ok_or("Could not extract filename from URL")?;
    let tarball_path = Path::new(filename);

    info!("Downloading tarball from {}", url);

    let lib_username = env::var("HOPS_LIB_USERNAME").unwrap_or_default();
    let lib_password = env::var("HOPS_LIB_PASSWORD").unwrap_or_default();

    let client = reqwest::blocking::Client::new();
    let mut request_builder = client.get(&url);
    if !lib_username.is_empty() && !lib_password.is_empty() {
        request_builder = request_builder.basic_auth(lib_username, Some(lib_password));
    }
    let mut response = request_builder.send()?;
    if !response.status().is_success() {
        return Err(format!("Failed to download file: HTTP {}", response.status()).into());
    }
    let mut tarball_file = File::create(&tarball_path)?;
    io::copy(&mut response, &mut tarball_file)?;
    info!("Downloaded tarball to {:?}", tarball_path);

    let extract_dir = PathBuf::from("temp_extracted");
    if extract_dir.exists() {
        fs::remove_dir_all(&extract_dir)?;
    }
    fs::create_dir(&extract_dir)?;

    let tarball_file = File::open(&tarball_path)?;
    let decompressor = flate2::read::GzDecoder::new(tarball_file);
    let mut archive = tar::Archive::new(decompressor);
    archive.unpack(&extract_dir)?;
    info!("Extracted tarball to {:?}", extract_dir);

    let subdirs: Vec<PathBuf> = fs::read_dir(&extract_dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_dir() {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    if subdirs.len() != 1 {
        return Err("Expected exactly one subdirectory in the extracted tarball".into());
    }

    let extracted_folder = &subdirs[0];
    let search_dir = extracted_folder.join("lib/native/libhdfs-golang");
    let mut options = CopyOptions::default();
    options.overwrite = true;
    options.content_only = true;
    fs_extra::dir::copy(&search_dir, lib_dir, &options)?;
    info!(
        "Copied library and header files to directory: {:?}",
        lib_dir
    );

    fs::remove_dir_all(&extract_dir)?;
    fs::remove_file(&tarball_path)?;
    info!("Cleaned up temporary files");
    Ok(())
}

#[cfg(target_os = "macos")]
fn set_libraries() {
    create_symlinks("macos".to_string(), false);
    println!("cargo:rustc-link-search=native=.");
    println!("cargo:rustc-link-lib=static=hdfs");
    println!("cargo:rustc-link-lib=framework=Security");
    println!("cargo:rustc-link-lib=framework=CoreFoundation");
}

#[cfg(target_os = "linux")]
fn set_libraries() {
    create_symlinks("linux".to_string(), true);
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    println!("cargo:rustc-link-search=native={}", manifest_dir);
    println!("cargo:rustc-link-lib=hdfs");
    println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn main() {
    panic!("Unsupported target OS: HopsFS object store only supports macOS and Linux.");
}

fn create_symlinks(target_os: String, shared: bool) {
    let lib_dir = Path::new("lib");

    let filter = match target_os.as_str() {
        "linux" => "linux-amd64",
        "macos" => "arm64",
        other => panic!("Unsupported target OS: {}", other),
    };

    let lib_ext = if shared {
        match target_os.as_str() {
            "linux" => ".so",
            "macos" => ".dylib",
            other => panic!("Unsupported target OS: {}", other),
        }
    } else {
        ".a"
    };

    let mut lib_file = None;
    let mut header_file = None;
    for entry in fs::read_dir(lib_dir).expect("Could not read lib directory") {
        let entry = entry.expect("Error reading directory entry");
        let file_name = entry.file_name().into_string().expect("Invalid file name");

        if file_name.ends_with(lib_ext) && file_name.contains(filter) {
            lib_file = Some(entry.path());
        } else if file_name.ends_with(".h") && file_name.contains(filter) {
            header_file = Some(entry.path());
        }
    }

    let lib_file = lib_file.expect("Library file not found");
    let header_file = header_file.expect("Header file not found");

    let symlink_lib_str = format!("libhdfs{}", lib_ext);
    let symlink_lib = Path::new(&symlink_lib_str);
    let symlink_header = Path::new("libhdfs.h");

    if symlink_lib.exists() {
        fs::remove_file(symlink_lib).expect("Failed to remove existing libhdfs.a symlink");
    }
    if symlink_header.exists() {
        fs::remove_file(symlink_header).expect("Failed to remove existing libhdfs.h symlink");
    }

    symlink(&lib_file, symlink_lib).expect("Failed to create symlink for library");
    symlink(&header_file, symlink_header).expect("Failed to create symlink for header");
}
