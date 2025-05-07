/// 代理配置模块
///
/// 集中管理所有代理设置，便于统一修改

/// 代理服务器地址
pub const PROXY_HOST: &str = "127.0.0.1";

/// 代理服务器端口
pub const PROXY_PORT: u16 = 1080;

/// 代理类型枚举
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProxyType {
    /// HTTP代理
    Http,
    /// SOCKS5代理
    Socks5,
}

/// 默认使用的代理类型
pub const DEFAULT_PROXY_TYPE: ProxyType = ProxyType::Socks5;

/// 获取完整的代理URL
pub fn get_proxy_url() -> String {
    match DEFAULT_PROXY_TYPE {
        ProxyType::Http => format!("http://{}:{}", PROXY_HOST, PROXY_PORT),
        ProxyType::Socks5 => format!("socks5://{}:{}", PROXY_HOST, PROXY_PORT),
    }
}

/// 代理配置结构体
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// 是否使用代理
    pub use_proxy: bool,
    /// 代理服务器地址
    pub host: String,
    /// 代理服务器端口
    pub port: u16,
    /// 代理类型
    pub proxy_type: ProxyType,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            use_proxy: true,
            host: PROXY_HOST.to_string(),
            port: PROXY_PORT,
            proxy_type: DEFAULT_PROXY_TYPE,
        }
    }
}

impl ProxyConfig {
    /// 创建新的代理配置
    pub fn new() -> Self {
        Self::default()
    }

    /// 创建指定类型的代理配置
    pub fn with_type(proxy_type: ProxyType) -> Self {
        Self {
            use_proxy: true,
            host: PROXY_HOST.to_string(),
            port: PROXY_PORT,
            proxy_type,
        }
    }

    /// 获取完整的代理URL
    pub fn get_url(&self) -> String {
        match self.proxy_type {
            ProxyType::Http => format!("http://{}:{}", self.host, self.port),
            ProxyType::Socks5 => format!("socks5://{}:{}", self.host, self.port),
        }
    }
}
