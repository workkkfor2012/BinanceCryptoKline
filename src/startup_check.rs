use crate::config::Config;
use crate::db::Database;
use crate::downloader::{Downloader, EXPECTED_COUNTS};
use crate::error::Result;
use log::{info, error};
use std::path::PathBuf;

/// 启动检查模块，用于检查数据库和历史K线数据
pub struct StartupCheck;

impl StartupCheck {
    /// 执行启动检查流程
    pub async fn run() -> Result<bool> {
        info!("执行启动检查流程...");

        // 检查数据库文件是否存在
        let db_path = PathBuf::from("./data/klines.db");
        let db_exists = db_path.exists();

        if !db_exists {
            info!("数据库文件不存在，将进行历史K线下载");
            // 执行下载流程
            Self::download_historical_klines().await?;
            return Ok(true); // 返回true表示已执行下载
        }

        // 检查BTC 1分钟K线数量
        let db = Database::new(&db_path)?;
        let btc_1m_count = db.get_kline_count("BTCUSDT", "1m")?;
        let btc_1w_count = db.get_kline_count("BTCUSDT", "1w")?;

        info!("BTC 1分钟K线数量: {}", btc_1m_count);
        info!("BTC 1周K线数量: {}", btc_1w_count);

        // 判断K线数量是否满足要求
        let min_1m_count = (EXPECTED_COUNTS[0] as f64 * 0.9) as i64; // 使用预期值的90%作为最小要求
        let min_1w_count = 1; // 1周K线至少要有1根

        if btc_1m_count < min_1m_count || btc_1w_count < min_1w_count {
            info!("BTC K线数量不满足要求，将进行历史K线下载");
            // 执行下载流程
            Self::download_historical_klines().await?;
            return Ok(true); // 返回true表示已执行下载
        }

        info!("启动检查完成，数据库和历史K线数据正常");
        Ok(false) // 返回false表示未执行下载
    }

    /// 下载历史K线数据
    async fn download_historical_klines() -> Result<()> {
        info!("开始下载历史K线数据...");

        // 初始化下载器配置
        let config = Config::new(
            "./data".to_string(),
            15,                                  // 并发数
            Some("1m,5m,30m,4h,1d,1w".to_string()), // K线周期
            None,                                // 起始时间（自动计算）
            None,                                // 结束时间（当前时间）
            None,                                // 交易对（下载所有U本位永续合约）
            true,                                // 使用SQLite文件存储
            Some(0),                             // 不限制K线数量
            false,                               // 不使用更新模式
        );

        // 创建下载器实例
        let downloader = Downloader::new(config)?;

        // 运行下载流程
        match downloader.run().await {
            Ok(_) => {
                info!("历史K线下载完成，数据已保存到数据库");
                Ok(())
            },
            Err(e) => {
                error!("历史K线下载失败: {}", e);
                Err(e.into())
            }
        }
    }
}
