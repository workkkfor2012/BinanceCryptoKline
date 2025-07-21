# K线聚合服务性能测试指南

## 概述

本项目实现了两种K线时钟处理算法：
- **标准版本**：传统线性扫描实现
- **SIMD版本**：使用SIMD批量比较和掩码跳过优化

## 快速开始

### 1. 启动标准版本
```powershell
.\scripts\start_standard.ps1
```

### 2. 启动SIMD版本
```powershell
.\scripts\start_simd.ps1
```

### 3. 自动性能对比测试
```powershell
# 运行10分钟对比测试
.\scripts\performance_comparison.ps1 -TestDurationMinutes 10

# 运行30分钟对比测试（推荐）
.\scripts\performance_comparison.ps1 -TestDurationMinutes 30
```

## 性能日志说明

### 统计性日志特点
- **统计周期**：每60秒输出一次汇总数据
- **日志目标**：`target='性能分析'`
- **日志类型**：`log_type='performance_summary'`

### 关键性能指标

| 字段名 | 说明 | 单位 |
|--------|------|------|
| `avg_duration_micros` | 平均处理时间 | 微秒 |
| `calls_per_second` | 每秒调用次数 | 次/秒 |
| `klines_scanned_per_second` | 每秒扫描K线数 | 条/秒 |
| `processing_efficiency` | 处理效率 | 百分比 |
| `total_processed_klines` | 总处理K线数 | 条 |

### 示例日志
```json
{
  "timestamp": "2024-01-20T10:30:00.123Z",
  "level": "INFO",
  "target": "性能分析",
  "log_type": "performance_summary",
  "worker_id": 0,
  "implementation": "SIMD",
  "total_calls": 3600,
  "avg_duration_micros": 245,
  "klines_scanned_per_second": 32800,
  "processing_efficiency": 0.8,
  "message": "【性能统计-60秒】Worker-0 SIMD 实现: 调用3600次 平均耗时245.1μs (120~890μs) 处理0.8条/次 扫描32800条/秒"
}
```

## 性能分析方法

### 1. 实时监控
```bash
# 过滤性能日志
tail -f logs/app.log | grep "performance_summary"

# 使用jq美化输出
tail -f logs/app.log | grep "performance_summary" | jq .
```

### 2. 离线分析
```bash
# 提取标准版本性能数据
grep "performance_summary" standard_performance.log | jq '.avg_duration_micros, .klines_scanned_per_second'

# 提取SIMD版本性能数据
grep "performance_summary" simd_performance.log | jq '.avg_duration_micros, .klines_scanned_per_second'
```

### 3. 对比分析
重点关注以下指标的对比：

1. **平均处理时间** (`avg_duration_micros`)
   - 越小越好
   - 反映单次时钟处理的效率

2. **扫描吞吐量** (`klines_scanned_per_second`)
   - 越大越好
   - 反映整体扫描性能

3. **处理效率** (`processing_efficiency`)
   - 反映实际处理的K线占总扫描K线的比例
   - 在相同业务负载下应该相近

## 测试建议

### 生产环境测试
1. **测试时长**：建议至少运行30分钟，获得足够的统计样本
2. **业务负载**：在相同的市场活跃度下进行测试
3. **系统资源**：监控CPU使用率、内存使用情况
4. **并发测试**：如果有多个Worker，观察不同Worker的性能表现

### 预期结果
根据测试环境的初步结果：
- **标准版本**：平均处理时间约244μs，扫描速度约32800条/秒
- **SIMD版本**：可能在特定场景下表现更好，需要实际测试验证

### 选择建议
- 如果标准版本性能已满足需求，建议使用标准版本（更简单、更稳定）
- 如果需要极致性能优化，可以测试SIMD版本的表现
- 可以通过feature flag在运行时灵活切换

## 故障排除

### 编译问题
```bash
# 检查标准版本编译
cargo check

# 检查SIMD版本编译
cargo check --features simd
```

### 运行问题
1. 确保配置文件 `config\BinanceKlineConfig.toml` 存在
2. 检查网络代理设置（如需要）
3. 查看错误日志进行诊断

### 性能异常
1. 检查系统资源使用情况
2. 确认测试环境的一致性
3. 观察业务负载是否相同
4. 检查是否有其他进程干扰

## 技术细节

### 标准实现
- 简单的线性扫描所有K线到期时间
- 对每个到期的K线进行处理
- 代码简洁，易于理解和维护

### SIMD实现
- 使用`wide`库进行4个i64的批量比较
- 通过掩码跳过无到期K线的块
- 理论上在大量K线同时到期时性能更好

### 选择依据
最终选择哪个版本应该基于实际的生产环境测试结果，而不是理论分析。
