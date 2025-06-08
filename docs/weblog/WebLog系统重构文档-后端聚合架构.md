# WebLog系统重构文档：后端聚合架构

## 1. 重构目标

将日志聚合逻辑从前端迁移到后端，实现：
- 后端负责所有日志处理和聚合逻辑
- 前端只负责显示和交互
- 页面刷新后状态保持
- 多客户端状态一致

## 2. 当前架构问题

### 2.1 前端聚合的问题
- ❌ 页面刷新后聚合状态丢失
- ❌ 大量日志处理影响前端性能
- ❌ 内存占用过大
- ❌ 多客户端状态不一致
- ❌ 重复计算浪费资源

### 2.2 需要保持的功能
- ✅ 实时原始日志显示
- ✅ 原始日志高频折叠（50%相似度，5秒窗口）
- ✅ 各个模块分类显示
- ✅ 滚动查看历史日志
- ✅ 点击查看详细JSON

## 3. 后端重构设计

### 3.1 数据结构设计

```rust
// 日志聚合器
pub struct LogAggregator {
    // 显示的日志条目（聚合后）
    displayed_logs: Vec<DisplayLogEntry>,
    // 消息频率统计
    message_frequency: HashMap<String, MessageFrequency>,
    // 已处理的日志ID
    processed_logs: HashSet<String>,
}

// 显示的日志条目
#[derive(Clone, Serialize)]
pub struct DisplayLogEntry {
    pub message: String,
    pub level: String,
    pub timestamp: String,
    pub count: usize,
    pub is_aggregated: bool,
    pub variations: Vec<String>, // 相似消息的变体
    pub all_logs: Vec<LogEntry>, // 包含的所有原始日志
}

// 消息频率统计
pub struct MessageFrequency {
    pub times: Vec<i64>, // 出现时间戳
    pub aggregated: bool,
    pub variations: Vec<String>,
}

// 模块聚合管理器
pub struct ModuleAggregatorManager {
    // 各模块的聚合器
    module_aggregators: HashMap<String, LogAggregator>,
    // 实时原始日志
    realtime_logs: VecDeque<String>,
    // 原始日志快照聚合器
    raw_snapshot_aggregator: LogAggregator,
}
```

### 3.2 核心功能模块

#### 3.2.1 相似度计算
```rust
pub fn calculate_similarity(str1: &str, str2: &str) -> f64 {
    // 实现与前端相同的相似度算法
    // 返回0.0-1.0的相似度值
}

pub fn find_similar_message(
    new_message: &str, 
    existing_messages: &HashMap<String, MessageFrequency>
) -> Option<(String, f64)> {
    // 查找相似度≥50%的已知消息
}
```

#### 3.2.2 日志聚合处理
```rust
impl LogAggregator {
    pub fn add_log_entry(&mut self, log_entry: LogEntry) -> bool {
        // 1. 检查是否已处理
        // 2. 查找相似消息
        // 3. 决定聚合或新增
        // 4. 更新显示列表
        // 5. 限制显示数量
    }
    
    pub fn cleanup_expired(&mut self, now: i64) {
        // 清理过期的频率记录和聚合数据
    }
    
    pub fn get_displayed_logs(&self, limit: usize) -> Vec<DisplayLogEntry> {
        // 返回用于显示的聚合日志
    }
}
```

#### 3.2.3 模块管理
```rust
impl ModuleAggregatorManager {
    pub fn process_module_logs(&mut self, module_name: &str, logs: Vec<LogEntry>) {
        // 处理模块日志
    }
    
    pub fn process_raw_log_snapshot(&mut self, logs: Vec<String>) {
        // 处理原始日志快照
    }
    
    pub fn get_module_data(&self, module_name: &str) -> Option<ModuleDisplayData> {
        // 获取模块显示数据
    }
    
    pub fn get_all_modules_data(&self) -> HashMap<String, ModuleDisplayData> {
        // 获取所有模块数据
    }
}
```

## 4. WebSocket消息格式

### 4.1 发送给前端的数据格式
```json
{
    "type": "dashboard_update",
    "data": {
        "uptime_seconds": 123,
        "health_score": 95,
        "module_logs": {
            "buffered_kline_store": {
                "total_logs": 150,
                "error_count": 2,
                "displayed_logs": [
                    {
                        "message": "缓冲区交换完成",
                        "level": "INFO",
                        "timestamp": "2024-01-01T12:00:00Z",
                        "count": 5,
                        "is_aggregated": true,
                        "variations": ["缓冲区交换完成: swap_count=1", "缓冲区交换完成: swap_count=2"],
                        "all_logs": [...]
                    }
                ]
            }
        },
        "realtime_log_data": {
            "recent_logs": ["log1", "log2", ...]
        },
        "raw_log_snapshot": {
            "timestamp": "2024-01-01T12:00:00Z",
            "total_count": 500,
            "displayed_logs": [
                {
                    "message": "初始化完成",
                    "level": "INFO",
                    "count": 3,
                    "is_aggregated": true,
                    "variations": [...],
                    "all_logs": [...]
                }
            ]
        }
    }
}
```

## 5. 前端重构设计

### 5.1 简化的前端职责
- ✅ 接收WebSocket数据
- ✅ 渲染显示界面
- ✅ 处理用户交互（展开/折叠JSON）
- ✅ 滚动查看历史
- ✅ 模块切换和过滤

### 5.2 前端数据处理
```javascript
// 简化的前端逻辑
function updateDashboard(data) {
    // 直接使用后端提供的聚合数据
    updateModuleLogs(data.module_logs);
    updateRealtimeLogs(data.realtime_log_data);
    updateRawLogSnapshot(data.raw_log_snapshot);
}

function updateModuleLogs(moduleLogs) {
    // 直接渲染后端提供的 displayed_logs
    for (const [moduleName, moduleData] of Object.entries(moduleLogs)) {
        renderModuleDisplayLogs(moduleName, moduleData.displayed_logs);
    }
}
```

## 6. 实现计划

### 6.1 第一阶段：后端聚合核心
1. **相似度计算模块**
   - 实现字符串相似度算法
   - 单元测试验证准确性

2. **日志聚合器**
   - 实现LogAggregator结构
   - 添加日志处理逻辑
   - 实现过期清理机制

3. **模块管理器**
   - 实现ModuleAggregatorManager
   - 集成到现有WebLog后端

### 6.2 第二阶段：API重构
1. **修改WebSocket消息格式**
   - 发送聚合后的数据
   - 保持向后兼容

2. **添加历史查询API**
   - 支持分页查询历史日志
   - 支持模块过滤

### 6.3 第三阶段：前端简化
1. **移除前端聚合逻辑**
   - 删除相似度计算代码
   - 删除聚合器相关代码

2. **简化渲染逻辑**
   - 直接渲染后端数据
   - 保持交互功能

### 6.4 第四阶段：优化和测试
1. **性能优化**
   - 内存使用优化
   - 处理速度优化

2. **功能测试**
   - 聚合准确性测试
   - 并发处理测试
   - 长时间运行测试

## 7. 技术疑问和建议

### 7.1 技术疑问

1. **持久化策略**
   - 聚合数据是否需要持久化到磁盘？
   - 重启后是否需要恢复聚合状态？
   - 建议：内存存储 + 定期快照

2. **性能考虑**
   - 大量日志时的处理性能如何保证？
   - 相似度计算的性能优化？
   - 建议：异步处理 + 批量聚合

3. **内存管理**
   - 如何控制内存使用不无限增长？
   - 历史日志的保留策略？
   - 建议：LRU淘汰 + 配置化限制

### 7.2 功能疑问

1. **聚合参数配置**
   - 相似度阈值（50%）是否需要可配置？
   - 时间窗口（5秒）是否需要可配置？
   - 显示数量限制是否需要可配置？

2. **多客户端同步**
   - 如何处理多个客户端的不同查看需求？
   - 是否需要支持客户端特定的过滤？
   - 建议：统一聚合 + 客户端过滤

3. **历史查询**
   - 是否需要支持时间范围查询？
   - 是否需要支持关键词搜索？
   - 是否需要支持导出功能？

### 7.3 实现建议

1. **渐进式迁移**
   - 先实现后端聚合，前端双模式支持
   - 验证功能正确性后再移除前端逻辑
   - 保持API向后兼容

2. **配置化设计**
   - 聚合参数可配置
   - 性能参数可调整
   - 功能开关可控制

3. **监控和调试**
   - 添加聚合性能指标
   - 添加内存使用监控
   - 保留详细的调试日志

## 8. 风险评估

### 8.1 技术风险
- **性能风险**：大量日志可能影响后端性能
- **内存风险**：聚合数据可能占用大量内存
- **兼容风险**：API变更可能影响现有功能

### 8.2 缓解措施
- **性能测试**：提前进行压力测试
- **内存限制**：设置合理的内存使用上限
- **渐进迁移**：保持向后兼容，逐步切换

## 9. 配置参数设计

### 9.1 聚合配置
```toml
[aggregation]
# 相似度阈值 (0.0-1.0)
similarity_threshold = 0.5
# 聚合时间窗口 (秒)
time_window = 5
# 最大显示日志数量
max_displayed_logs = 20
# 最大历史保留数量
max_history_logs = 1000

[performance]
# 批量处理大小
batch_size = 100
# 清理间隔 (秒)
cleanup_interval = 60
# 最大内存使用 (MB)
max_memory_mb = 512
```

### 9.2 功能开关
```toml
[features]
# 启用高频日志聚合
enable_aggregation = true
# 启用实时日志
enable_realtime_logs = true
# 启用原始日志快照
enable_raw_snapshot = true
# 启用性能监控
enable_performance_monitoring = true
```

## 10. 监控指标

### 10.1 性能指标
- 日志处理速度 (logs/second)
- 聚合处理延迟 (ms)
- 内存使用量 (MB)
- CPU使用率 (%)

### 10.2 业务指标
- 活跃模块数量
- 聚合日志数量
- 相似度匹配率
- 客户端连接数

## 11. 测试策略

### 11.1 单元测试
- 相似度计算准确性
- 聚合逻辑正确性
- 过期清理机制
- 配置参数验证

### 11.2 集成测试
- WebSocket消息格式
- 多模块并发处理
- 前后端数据一致性
- 错误处理和恢复

### 11.3 性能测试
- 大量日志处理能力
- 内存使用稳定性
- 长时间运行稳定性
- 并发客户端支持

## 12. 部署和运维

### 12.1 部署要求
- Rust 1.70+ 环境
- 内存：建议 1GB+
- CPU：建议 2核+
- 网络：WebSocket支持

### 12.2 运维监控
- 日志文件轮转
- 性能指标收集
- 错误告警机制
- 自动重启策略

---

**文档版本**: v1.0
**创建时间**: 2024年12月
**最后更新**: 2024年12月
**负责人**: AI Assistant
