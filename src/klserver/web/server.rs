// Web服务器实现
use std::net::SocketAddr;
use std::sync::Arc;
use axum::{
    routing::get,
    Router,
};
use tower_http::services::ServeDir;
use crate::klcommon::{Database, Result, AppError};
use tracing::{info, error};

use super::handlers;

/// 启动Web服务器
pub async fn start_web_server(db: Arc<Database>) -> Result<()> {
    info!("Starting web server...");

    // 创建静态文件服务
    let static_service = ServeDir::new("static");
    info!("Static file service created for directory: static");

    // 创建路由
    let app = Router::new()
        .route("/", get(|| async {
            let index_html = tokio::fs::read_to_string("static/index.html").await.unwrap_or_else(|_| "<html><body><h1>Error: Could not load index.html</h1></body></html>".to_string());
            axum::response::Html(index_html)
        }))
        .route("/test", get(|| async { "Hello, World from test!" }))
        .route("/api/klines/:symbol/:interval", get(handlers::klines_handler))
        .nest_service("/static", static_service)
        .with_state(db);
    info!("Router created with routes: /, /test, /api/klines/:symbol/:interval, /static");

    // 绑定地址
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    info!("Binding to address: {}", addr);

    info!("Web服务器启动在 http://{}", addr);

    // 启动服务器
    info!("Web服务器开始绑定到地址: {}", addr);

    // 使用axum 0.7的新API启动服务器
    info!("Starting server with axum 0.7 API...");
    let listener = tokio::net::TcpListener::bind(&addr).await
        .map_err(|e| AppError::WebServerError(format!("绑定地址失败: {}", e)))?;

    match axum::serve(listener, app).await {
        Ok(_) => info!("Web服务器已关闭"),
        Err(e) => {
            error!("Web服务器错误: {}", e);
            return Err(AppError::WebServerError(format!("Web服务器错误: {}", e)));
        }
    }

    Ok(())
}

