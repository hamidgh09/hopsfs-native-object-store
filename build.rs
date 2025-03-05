fn main() {
    println!("cargo:rustc-link-lib=static=hdfs");
    println!("cargo:rustc-link-search=native=."); 
    #[cfg(target_os = "macos")] 
    {
        println!("cargo:rustc-link-lib=framework=Security");
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
    }
}
