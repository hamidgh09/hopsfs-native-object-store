use std::env;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::os::unix::fs::symlink;

fn main() -> Result<(), Box<dyn std::error::Error>> {

    if env::var("CARGO_FEATURE_USE_LATEST_LIB").is_ok() {
        extract_tarball()?;
    }

    set_libraries();
    Ok(())
}

fn extract_tarball() -> Result<(), Box<dyn std::error::Error>> {
    let lib_dir = Path::new("lib");
    if lib_dir.exists() {
        let found = fs::read_dir(lib_dir)?
            .filter_map(Result::ok)
            .any(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("a"));
        if found {
            println!("The lib directory is not empty, skipping download. \n\
            If you want to force a download, delete the lib directory and re-run the build script.");
            set_libraries();
            return Ok(());
        }
    } else {
        fs::create_dir(lib_dir)?;
    }

    let url = env::var("HOPS_LIB_URL")
        .expect("Please set the HOPS_LIB_URL environment variable.");

    let parsed_url = url::Url::parse(&url)?;
    let filename = parsed_url
        .path_segments()
        .and_then(|segments| segments.last())
        .and_then(|name| if name.is_empty() { None } else { Some(name) })
        .ok_or("Could not extract filename from URL")?;
    let tarball_path = Path::new(filename);

    println!("Downloading tarball from {}", url);
    let mut response = reqwest::blocking::get(&url)?;
    if !response.status().is_success() {
        return Err(format!("Failed to download file: HTTP {}", response.status()).into());
    }
    let mut tarball_file = File::create(&tarball_path)?;
    io::copy(&mut response, &mut tarball_file)?;
    println!("Downloaded tarball to {:?}", tarball_path);

    let extract_dir = PathBuf::from("temp_extracted");
    if extract_dir.exists() {
        fs::remove_dir_all(&extract_dir)?;
    }
    fs::create_dir(&extract_dir)?;

    let is_gzipped = tarball_path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext_str| ext_str.eq_ignore_ascii_case("gz"))
        .unwrap_or(false);

    let tarball_file = File::open(&tarball_path)?;
    if is_gzipped {
        let decompressor = flate2::read::GzDecoder::new(tarball_file);
        let mut archive = tar::Archive::new(decompressor);
        archive.unpack(&extract_dir)?;
    } else {
        let mut archive = tar::Archive::new(tarball_file);
        archive.unpack(&extract_dir)?;
    }
    println!("Extracted tarball to {:?}", extract_dir);

    let mut lib_file_path: Option<PathBuf> = None;
    for entry in walkdir::WalkDir::new(&extract_dir) {
        let entry = entry?;
        if entry.file_type().is_file() {
            if let Some(ext) = entry.path().extension() {
                if ext == "a" {
                    lib_file_path = Some(entry.path().to_path_buf());
                    break;
                }
            }
        }
    }

    let lib_file_path = lib_file_path.ok_or("No .a file found in extracted archive")?;
    println!("Found library file at {:?}", lib_file_path);

    //TODO: Fix this
    /*
    let destination = Path::new(
        lib_file_path
            .file_name()
            .ok_or("Failed to get library filename")?,
    );
    fs::copy(&lib_file_path, &destination)?;
    println!("Copied library file to {:?}", destination);
    */
    // Cleanup: remove the extracted directory and downloaded tarball.
    fs::remove_dir_all(&extract_dir)?;
    fs::remove_file(&tarball_path)?;
    println!("Cleaned up temporary files");
    Ok(())
}

#[cfg(target_os = "macos")]
fn set_libraries() {
    create_symlinks("macos".to_string());
    println!("cargo:rustc-link-search=native=.");
    println!("cargo:rustc-link-lib=static=hdfs");
    println!("cargo:rustc-link-lib=framework=Security");
    println!("cargo:rustc-link-lib=framework=CoreFoundation");

}

#[cfg(target_os = "linux")]
fn set_libraries() {
    create_symlinks("linux".to_string());
    println!("cargo:rustc-link-search=native=.");
    println!("cargo:rustc-link-lib=static=hdfs");
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn main() {
    panic!("Unsupported target OS: HopsFS object store only supports macOS and Linux.");
}

fn create_symlinks(target_os: String) {
    let lib_dir = Path::new("lib");

    let filter = match target_os.as_str() {
        "linux" => "linux-amd64",
        "macos" => "arm64",
        other => panic!("Unsupported target OS: {}", other),
    };

    let mut lib_file = None;
    let mut header_file = None;
    for entry in fs::read_dir(lib_dir).expect("Could not read lib directory") {
        let entry = entry.expect("Error reading directory entry");
        let file_name = entry.file_name().into_string().expect("Invalid file name");

        if file_name.ends_with(".a") && file_name.contains(filter) {
            lib_file = Some(entry.path());
        } else if file_name.ends_with(".h") && file_name.contains(filter) {
            header_file = Some(entry.path());
        }
    }

    let lib_file = lib_file.expect("Library file not found");
    let header_file = header_file.expect("Header file not found");

    let symlink_lib = Path::new("libhdfs.a");
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
