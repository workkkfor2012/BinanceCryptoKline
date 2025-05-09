// Symbol分区器 - 将交易对分配到不同的WebSocket连接
use log::debug;

/// 将交易对列表分区到指定数量的分区中
/// 
/// # 参数
/// * `all_symbols` - 所有交易对列表
/// * `num_partitions` - 分区数量
/// 
/// # 返回值
/// * 分区后的交易对列表，每个分区是一个Vec<String>
pub fn partition_symbols(all_symbols: &[String], num_partitions: usize) -> Vec<Vec<String>> {
    if num_partitions == 0 || all_symbols.is_empty() {
        debug!("无法分区: 分区数量为0或交易对列表为空");
        return vec![all_symbols.to_vec()]; // 或者返回空vec，取决于期望行为
    }
    
    let mut partitions = vec![Vec::new(); num_partitions];
    
    for (i, symbol) in all_symbols.iter().enumerate() {
        partitions[i % num_partitions].push(symbol.clone());
    }
    
    // 输出分区信息
    for (i, partition) in partitions.iter().enumerate() {
        debug!("分区 {}: {} 个交易对", i + 1, partition.len());
    }
    
    partitions
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_partition_symbols() {
        // 测试用例1: 正常分区
        let symbols = vec![
            "BTCUSDT".to_string(), 
            "ETHUSDT".to_string(), 
            "BNBUSDT".to_string(),
            "SOLUSDT".to_string(),
            "ADAUSDT".to_string()
        ];
        
        let partitions = partition_symbols(&symbols, 2);
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0], vec!["BTCUSDT".to_string(), "BNBUSDT".to_string(), "ADAUSDT".to_string()]);
        assert_eq!(partitions[1], vec!["ETHUSDT".to_string(), "SOLUSDT".to_string()]);
        
        // 测试用例2: 分区数量为0
        let partitions = partition_symbols(&symbols, 0);
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], symbols);
        
        // 测试用例3: 空交易对列表
        let empty_symbols: Vec<String> = vec![];
        let partitions = partition_symbols(&empty_symbols, 3);
        assert_eq!(partitions.len(), 1);
        assert!(partitions[0].is_empty());
        
        // 测试用例4: 分区数量大于交易对数量
        let partitions = partition_symbols(&symbols, 10);
        assert_eq!(partitions.len(), 10);
        assert_eq!(partitions[0], vec!["BTCUSDT".to_string()]);
        assert_eq!(partitions[1], vec!["ETHUSDT".to_string()]);
        assert_eq!(partitions[2], vec!["BNBUSDT".to_string()]);
        assert_eq!(partitions[3], vec!["SOLUSDT".to_string()]);
        assert_eq!(partitions[4], vec!["ADAUSDT".to_string()]);
        assert!(partitions[5].is_empty());
    }
}
