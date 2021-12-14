

fn main() {
    println!("cargo:rustc-link-search=../../../bazel-bin/external/libmemcached/copy_libmemcached/libmemcached/lib");
    println!("cargo:rustc-link-lib=memcached");

    println!("cargo:rustc-link-lib=dl");
    println!("cargo:rustc-link-lib=mariadbcpp");

    println!("cargo:rustc-link-search=../../../bazel-bin/baseline");
    println!("cargo:rustc-link-lib=mymemcached");
}