// Actor模型数据结构 - 用于K线合成系统
use crate::klcommon::{Database, Result};
use crate::klcommon::websocket::MessageHandler;
// 移除未使用的导入
use std::sync::Arc;
use std::future::Future;

// 临时的消息处理器，用于替代旧的AggTradeMessageHandler
pub struct DummyMessageHandler {
    pub db: Arc<Database>,
}

impl MessageHandler for DummyMessageHandler {
    fn handle_message(&self, _connection_id: usize, _text: String) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

// 注意：所有K线相关的数据结构已移至 src\klcommon\models.rs
// 这里只重新导出它们，以保持向后兼容性
