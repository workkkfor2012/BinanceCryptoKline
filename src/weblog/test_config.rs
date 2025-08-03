use serde::Deserialize;

/// WebLog配置结构
#[derive(Deserialize)]
struct WebLogLoggingConfig {
    weblog: WebLogServiceConfig,
}

#[derive(Deserialize)]
struct WebLogServiceConfig {
    log_level: String,
}

/// 读取WebLog日志级别配置 - 从weblog专用配置文件读取
fn load_weblog_log_level() -> String {
    // weblog专用配置文件路径（相对于weblog根目录）
    let config_path = std::path::PathBuf::from("config/logging_config.toml");
    
    if let Ok(content) = std::fs::read_to_string(&config_path) {
        // 解析TOML配置文件
        match toml::from_str::<WebLogLoggingConfig>(&content) {
            Ok(config) => {
                println!("从weblog专用配置文件读取日志级别: {} (路径: {:?})", 
                         config.weblog.log_level, config_path);
                return config.weblog.log_level;
            }
            Err(e) => {
                println!("解析weblog配置文件失败: {} (路径: {:?})", e, config_path);
            }
        }
    } else {
        println!("无法读取weblog配置文件: {:?}", config_path);
    }

    // 配置文件读取失败，使用默认值
    println!("使用默认日志级别: info");
    "info".to_string()
}

fn main() {
    println!("测试weblog配置读取功能");
    let log_level = load_weblog_log_level();
    println!("最终日志级别: {}", log_level);
}
