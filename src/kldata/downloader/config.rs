use crate::klcommon::Result;
use std::path::PathBuf;

/// 下载器配置
pub struct Config {
    /// 输出目录
    pub output_dir: PathBuf,
    /// 并发下载数
    pub concurrency: usize,
    /// K线周期
    pub intervals: Vec<String>,
    /// 起始时间
    pub start_time: Option<i64>,
    /// 结束时间
    pub end_time: Option<i64>,
    /// 交易对列表
    pub symbols: Option<Vec<String>>,
    /// 每个请求的最大K线数量
    pub max_limit: i32,
    /// 每个交易对保留的最大K线数量
    pub max_klines_per_symbol: usize,
    /// 是否使用SQLite
    pub use_sqlite: bool,
    /// 数据库路径
    pub db_path: Option<PathBuf>,
    /// 是否仅更新
    pub update_only: bool,
}

impl Config {
    /// 创建新的配置实例
    pub fn new(
        output_dir: Option<String>,
        concurrency: Option<usize>,
        intervals: Option<Vec<String>>,
        start_time: Option<i64>,
        end_time: Option<i64>,
        symbols: Option<Vec<String>>,
        use_sqlite: Option<bool>,
        max_klines_per_symbol: Option<usize>,
        update_only: Option<bool>,
    ) -> Self {
        // 设置默认输出目录
        let output_dir = PathBuf::from(output_dir.unwrap_or_else(|| "./data".to_string()));

        // 设置默认并发数
        let concurrency = concurrency.unwrap_or(10);

        // 设置默认K线周期
        let intervals = intervals.unwrap_or_else(|| {
            vec![
                "1m".to_string(),
                "5m".to_string(),
                "30m".to_string(),
                "4h".to_string(),
                "1d".to_string(),
                "1w".to_string(),
            ]
        });

        // 设置起始时间（如果指定）
        let start_time = start_time;

        // 设置结束时间（如果指定，否则为当前时间）
        let end_time = end_time;

        // 设置默认每个交易对保留的最大K线数量
        let max_klines_per_symbol = max_klines_per_symbol.unwrap_or(0);

        // 设置默认是否使用SQLite
        let use_sqlite = use_sqlite.unwrap_or(true);

        // 设置默认数据库路径
        let db_path = if use_sqlite {
            Some(output_dir.join("klines.db"))
        } else {
            None
        };

        // 设置默认是否仅更新
        let update_only = update_only.unwrap_or(false);

        Self {
            output_dir,
            concurrency,
            intervals,
            start_time,
            end_time,
            symbols,
            max_limit: 1000,
            max_klines_per_symbol,
            use_sqlite,
            db_path,
            update_only,
        }
    }

    /// 确保输出目录存在
    pub fn ensure_output_dir(&self) -> Result<()> {
        if !self.output_dir.exists() {
            std::fs::create_dir_all(&self.output_dir)?;
        }
        Ok(())
    }

    /// 获取特定交易对和周期的输出路径
    pub fn get_output_path(&self, symbol: &str, interval: &str) -> PathBuf {
        let filename = format!("{}_{}_{}.csv",
            symbol.to_lowercase(),
            interval,
            chrono::Utc::now().format("%Y%m%d"));

        self.output_dir.join(filename)
    }
}
