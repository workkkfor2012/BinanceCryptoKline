# SQLite优化脚本 - 使SQLite更接近内存数据库性能
# 此脚本用于测试和应用最佳的SQLite配置，以提高高频数据写入性能

# 设置UTF-8编码
$OutputEncoding = [System.Text.Encoding]::UTF8; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 定义颜色函数
function Write-ColorOutput($ForegroundColor) {
    $fc = $host.UI.RawUI.ForegroundColor
    $host.UI.RawUI.ForegroundColor = $ForegroundColor
    if ($args) {
        Write-Output $args
    }
    $host.UI.RawUI.ForegroundColor = $fc
}

function Write-Green($text) { Write-ColorOutput Green $text }
function Write-Yellow($text) { Write-ColorOutput Yellow $text }
function Write-Red($text) { Write-ColorOutput Red $text }
function Write-Cyan($text) { Write-ColorOutput Cyan $text }

# 检查SQLite工具是否存在
$sqlitePath = "F:\work\tool\sqlite-tools-win-x64-3490100\sqlite3.exe"
if (-not (Test-Path $sqlitePath)) {
    Write-Red "SQLite工具不存在: $sqlitePath"
    Write-Yellow "请修改脚本中的SQLite路径或安装SQLite工具"
    exit 1
}

# 检查数据库文件
$dbPath = "..\data\klines.db"
if (-not (Test-Path $dbPath)) {
    Write-Yellow "数据库文件不存在: $dbPath"
    Write-Yellow "将创建一个新的数据库文件用于测试"
    
    # 创建data目录（如果不存在）
    if (-not (Test-Path "..\data")) {
        New-Item -ItemType Directory -Path "..\data" | Out-Null
    }
}

# 显示当前配置
Write-Green "===== 当前SQLite配置 ====="
& $sqlitePath $dbPath "PRAGMA journal_mode; PRAGMA synchronous; PRAGMA cache_size; PRAGMA mmap_size; PRAGMA temp_store; PRAGMA wal_autocheckpoint; PRAGMA busy_timeout; PRAGMA page_size; PRAGMA locking_mode;"

# 显示优化建议
Write-Green "`n===== SQLite优化建议 ====="
Write-Cyan "1. 内存模式优化"
Write-Yellow "   - 将journal_mode设置为MEMORY (比WAL更快，但崩溃恢复能力较弱)"
Write-Yellow "   - 将synchronous设置为OFF (最高性能，但断电可能丢失数据)"
Write-Yellow "   - 增加cache_size至少到1GB (-1048576 KB)"
Write-Yellow "   - 增加mmap_size至少到1GB (1073741824 bytes)"
Write-Yellow "   - 将temp_store设置为MEMORY (临时表存储在内存中)"
Write-Yellow "   - 将page_size增加到8192或16384 (更大的页面大小，减少I/O操作)"
Write-Yellow "   - 将locking_mode设置为EXCLUSIVE (独占锁定，减少锁竞争)"

Write-Cyan "2. 写入队列优化"
Write-Yellow "   - 已实现的写入队列机制是一个很好的优化"
Write-Yellow "   - 考虑增加队列大小以缓冲更多写入操作"
Write-Yellow "   - 考虑使用多个写入线程处理队列"

Write-Cyan "3. 表结构优化"
Write-Yellow "   - 考虑减少索引数量，只保留必要的索引"
Write-Yellow "   - 考虑使用INTEGER类型而非TEXT存储数值，以减少存储空间和提高性能"

# 询问是否应用优化
Write-Green "`n是否应用内存模式优化配置? (Y/N)"
$confirm = Read-Host
if ($confirm -eq "Y" -or $confirm -eq "y") {
    Write-Yellow "正在应用内存模式优化配置..."
    
    # 创建优化SQL
    $optimizeSql = @"
PRAGMA journal_mode = MEMORY;
PRAGMA synchronous = OFF;
PRAGMA cache_size = -1048576;
PRAGMA mmap_size = 1073741824;
PRAGMA temp_store = MEMORY;
PRAGMA page_size = 8192;
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA busy_timeout = 10000;
"@
    
    # 将优化SQL写入临时文件
    $tempFile = [System.IO.Path]::GetTempFileName()
    $optimizeSql | Out-File -FilePath $tempFile -Encoding utf8
    
    # 应用优化
    & $sqlitePath $dbPath ".read $tempFile"
    
    # 删除临时文件
    Remove-Item $tempFile
    
    # 显示优化后的配置
    Write-Green "`n===== 优化后的SQLite配置 ====="
    & $sqlitePath $dbPath "PRAGMA journal_mode; PRAGMA synchronous; PRAGMA cache_size; PRAGMA mmap_size; PRAGMA temp_store; PRAGMA wal_autocheckpoint; PRAGMA busy_timeout; PRAGMA page_size; PRAGMA locking_mode;"
    
    # 生成修改代码的建议
    Write-Green "`n===== 代码修改建议 ====="
    Write-Yellow "请修改src\klcommon\db.rs中的Database::new方法，将PRAGMA配置更新为以下内容:"
    Write-Cyan @"
let manager = SqliteConnectionManager::file(db_path).with_init(|conn| {
    conn.execute_batch("
        PRAGMA journal_mode = MEMORY;       -- 使用内存日志模式，比WAL更快
        PRAGMA synchronous = OFF;           -- 关闭同步，最高性能
        PRAGMA cache_size = -1048576;       -- 设置缓存为1GB (负数表示KB)
        PRAGMA mmap_size = 1073741824;      -- 1GB内存映射
        PRAGMA temp_store = MEMORY;         -- 临时表存储在内存中
        PRAGMA busy_timeout = 10000;        -- 10秒忙等待超时
        PRAGMA page_size = 8192;            -- 更大的页面大小
        PRAGMA locking_mode = EXCLUSIVE;    -- 独占锁定模式
    ")
});
"@
    
    Write-Yellow "`n同时，考虑增加写入队列的大小:"
    Write-Cyan "let (sender, receiver) = bounded(5000); // 增加队列容量到5000"
    
    Write-Yellow "`n警告: 这些设置会大幅提高性能，但会降低数据安全性。在断电或崩溃时可能丢失数据。"
    Write-Yellow "如果数据安全性比性能更重要，请考虑使用以下折中方案:"
    Write-Cyan @"
let manager = SqliteConnectionManager::file(db_path).with_init(|conn| {
    conn.execute_batch("
        PRAGMA journal_mode = WAL;          -- 保留WAL模式以获得更好的安全性
        PRAGMA synchronous = NORMAL;        -- 平衡性能和安全性
        PRAGMA cache_size = -1048576;       -- 设置缓存为1GB (负数表示KB)
        PRAGMA mmap_size = 1073741824;      -- 1GB内存映射
        PRAGMA temp_store = MEMORY;         -- 临时表存储在内存中
        PRAGMA wal_autocheckpoint = 1000;   -- 每1000页检查点
        PRAGMA busy_timeout = 10000;        -- 10秒忙等待超时
        PRAGMA page_size = 8192;            -- 更大的页面大小
    ")
});
"@
} else {
    Write-Yellow "未应用优化配置。"
}

# 显示内存数据库选项
Write-Green "`n===== 内存数据库选项 ====="
Write-Yellow "如果您希望使用真正的内存数据库，有以下选项:"

Write-Cyan "1. 纯内存数据库 (每次启动都是空的)"
Write-Yellow "   - 使用':memory:'作为数据库路径"
Write-Yellow "   - 优点: 最高性能"
Write-Yellow "   - 缺点: 程序关闭后数据丢失，无法持久化"
Write-Yellow "   - 代码示例:"
Write-Cyan @"
let manager = SqliteConnectionManager::memory().with_init(|conn| {
    conn.execute_batch("
        PRAGMA synchronous = OFF;
        PRAGMA cache_size = -1048576;
        PRAGMA temp_store = MEMORY;
    ")
});
"@

Write-Cyan "`n2. 内存映射数据库 (启动时从磁盘加载，关闭时保存)"
Write-Yellow "   - 在启动时将整个数据库加载到内存中"
Write-Yellow "   - 在关闭时将内存中的数据保存到磁盘"
Write-Yellow "   - 优点: 接近内存数据库的性能，同时保留数据持久性"
Write-Yellow "   - 缺点: 需要额外的代码来管理加载和保存过程"
Write-Yellow "   - 代码示例:"
Write-Cyan @"
// 在启动时:
let db_path = Path::new("./data/klines.db");
let memory_db = ":memory:";

// 创建内存数据库连接
let manager = SqliteConnectionManager::memory().with_init(|conn| {
    // 应用优化设置
    conn.execute_batch("
        PRAGMA synchronous = OFF;
        PRAGMA cache_size = -1048576;
        PRAGMA temp_store = MEMORY;
    ").unwrap();
    
    // 如果磁盘数据库存在，则加载它
    if db_path.exists() {
        // 从磁盘数据库备份到内存数据库
        let disk_conn = rusqlite::Connection::open(db_path).unwrap();
        disk_conn.backup(conn, "main", "main").unwrap();
    }
    
    Ok(())
});

// 在关闭时:
// 将内存数据库保存到磁盘
let conn = pool.get().unwrap();
let disk_conn = rusqlite::Connection::open(db_path).unwrap();
conn.backup(&disk_conn, "main", "main").unwrap();
"@

Write-Green "`n脚本执行完成。"
