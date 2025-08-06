// kline_server/build.rs

use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let profile = env::var("PROFILE").expect("PROFILE 未设置");
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR 未设置");
    let target_dir = Path::new(&manifest_dir).join("target").join(profile);

    // --- 定义所有需要拷贝的资源 ---
    let resources_to_copy: [&str; 0] = [
        // 如果未来还有其他dll，直接在这里加
    ];

    for resource_name in &resources_to_copy {
        // 定义源路径和目标路径
        let src_path = Path::new(&manifest_dir).join("res/lib").join(resource_name);
        let dest_path = target_dir.join(resource_name);

        // 执行拷贝
        if src_path.exists() {
            fs::copy(&src_path, &dest_path)
                .expect(&format!("拷贝 {} 从 {:?} 到 {:?} 失败", resource_name, src_path, dest_path));
            
            println!("cargo:info=已将 {} 拷贝到目标目录: {:?}", resource_name, dest_path);
        } else {
            println!("cargo:warning=未找到资源文件 {} 于 {:?}，跳过拷贝。", resource_name, src_path);
        }

        // 告诉 Cargo 监控这个文件
        println!("cargo:rerun-if-changed={}", src_path.strip_prefix(&manifest_dir).unwrap().to_str().unwrap().replace('\\', "/"));
    }

    // 告诉 Cargo 监控 build.rs 本身
    println!("cargo:rerun-if-changed=build.rs");
}