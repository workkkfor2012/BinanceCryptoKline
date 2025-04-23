// 导入测试模块
mod aggregator_test;
mod downloader_test;
mod streamer_test;
mod web_test;

// 主测试函数
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
