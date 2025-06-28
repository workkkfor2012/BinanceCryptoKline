//! 错误分类系统演示
//! 
//! 展示如何在实际代码中使用新的错误分类和追踪系统

use kline_server::klcommon::error::{AppError, Result};
use kline_server::klcommon::context::{AppTraceContext, TraceContext, init_tracing_config};
use tracing::{info, error, instrument};
use tokio::sync::mpsc;

/// 模拟API请求任务
#[derive(Debug, Clone)]
struct ApiTask {
    url: String,
    context: AppTraceContext,
}

/// 模拟API客户端
#[derive(Debug)]
struct MockApiClient;

impl MockApiClient {
    #[instrument(ret, err)]
    async fn fetch_data(&self, url: &str) -> Result<String> {
        info!("开始请求API: {}", url);
        
        // 模拟不同类型的错误
        match url {
            "http://timeout.example.com" => {
                error!(
                    message = "API请求超时",
                    error.summary = "api_request_failed",
                    error.details = "连接超时: 5秒内无响应"
                );
                Err(AppError::ApiError("连接超时".to_string()))
            },
            "http://invalid.json.com" => {
                let json_error = serde_json::from_str::<serde_json::Value>("{invalid").unwrap_err();
                error!(
                    message = "JSON解析失败",
                    error.summary = json_error.to_string(),
                    error.details = %json_error
                );
                Err(AppError::JsonError(json_error))
            },
            "http://database.error.com" => {
                let db_error = AppError::DatabaseError("数据库连接失败".to_string());
                error!(
                    message = "数据库操作失败",
                    error.summary = db_error.get_error_type_summary(),
                    error.details = %db_error
                );
                Err(db_error)
            },
            _ => {
                info!("API请求成功");
                Ok(format!("数据来自: {}", url))
            }
        }
    }
    
    #[instrument(skip(self, task), fields(url = %task.url), ret, err)]
    async fn process_task(&self, task: ApiTask) -> Result<String> {
        // 在任务的上下文中执行API请求
        TraceContext::instrument(&task.context, async {
            self.fetch_data(&task.url).await
        }).await
    }
}

/// 错误重试逻辑演示
#[instrument(skip(client, task), fields(url = %task.url), ret, err)]
async fn retry_with_classification(client: &MockApiClient, task: ApiTask, max_retries: usize) -> Result<String> {
    let mut last_error = None;
    
    for attempt in 1..=max_retries {
        info!("尝试第 {} 次请求", attempt);
        
        match client.process_task(task.clone()).await {
            Ok(result) => {
                info!("请求成功，尝试次数: {}", attempt);
                return Ok(result);
            },
            Err(e) => {
                let error_summary = e.get_error_type_summary();
                let is_retryable = e.is_retryable();
                
                error!(
                    message = "请求失败",
                    attempt = attempt,
                    error.summary = error_summary,
                    error.details = %e,
                    is_retryable = is_retryable
                );
                
                if !is_retryable {
                    info!("错误不可重试，停止尝试");
                    return Err(e);
                }
                
                if attempt < max_retries {
                    info!("错误可重试，等待后重试");
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                
                last_error = Some(e);
            }
        }
    }
    
    Err(last_error.unwrap())
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化追踪系统
    init_tracing_config(true);
    
    // 初始化日志
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();
    
    info!("错误分类系统演示开始");
    
    let client = MockApiClient;
    let (sender, mut receiver) = mpsc::channel(10);
    
    // 创建不同类型的测试任务
    let test_urls = vec![
        "http://success.example.com",      // 成功案例
        "http://timeout.example.com",      // 可重试错误
        "http://invalid.json.com",         // 不可重试错误
        "http://database.error.com",       // 可重试错误
    ];
    
    // 发送任务到队列
    for url in test_urls {
        let task = ApiTask {
            url: url.to_string(),
            context: AppTraceContext::new(),
        };
        sender.send(task).await.unwrap();
    }
    drop(sender);
    
    // 处理任务队列
    while let Some(task) = receiver.recv().await {
        info!("开始处理任务: {}", task.url);
        
        match retry_with_classification(&client, task, 3).await {
            Ok(result) => {
                info!("任务完成: {}", result);
            },
            Err(e) => {
                error!(
                    message = "任务最终失败",
                    error.summary = e.get_error_type_summary(),
                    error.details = %e,
                    is_retryable = e.is_retryable()
                );
            }
        }
        
        println!("---");
    }
    
    info!("错误分类系统演示结束");
    Ok(())
}
