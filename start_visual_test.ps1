# 启动可视化测试服务器
# 注意：现在需要在 src\bin\klagg_sub_threads.rs 文件开头修改 VISUAL_TEST_MODE 常量为 true

Write-Host "启动可视化测试服务器..." -ForegroundColor Green
Write-Host "注意：请确保在 src\bin\klagg_sub_threads.rs 中设置 VISUAL_TEST_MODE = true" -ForegroundColor Red
Write-Host "访问地址: http://localhost:9090" -ForegroundColor Yellow
Write-Host "按 Ctrl+C 停止服务器" -ForegroundColor Cyan

# 启动服务器
cargo run --release --bin klagg_sub_threads
