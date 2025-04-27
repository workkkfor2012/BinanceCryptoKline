#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BTC K线数据监控脚本
- 每250毫秒读取一次数据库中的BTC K线数据
- 提取并打印收盘价和成交量到控制台
- 从环境变量中读取周期
"""

import sqlite3
import time
from datetime import datetime
import os
import logging

# 从环境变量中读取周期，默认为4h
PERIOD = os.environ.get("BTC_PERIOD", "4h")

# 从环境变量中读取监控持续时间（秒），默认为60秒
DURATION = int(os.environ.get("BTC_DURATION", "60"))

# 配置日志
log_file = f"btc_{PERIOD}_monitor.log"
logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - [DB-{PERIOD}] - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(f"db_monitor_{PERIOD}")

# 数据库配置
DB_PATH = "./data/klines.db"
TABLE_NAME = f"k_btc_{PERIOD}"
QUERY_INTERVAL = 0.25  # 250毫秒

def format_timestamp(timestamp_ms):
    """将毫秒时间戳格式化为可读时间"""
    dt = datetime.fromtimestamp(timestamp_ms / 1000)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def connect_to_database():
    """连接到SQLite数据库"""
    if not os.path.exists(DB_PATH):
        logger.error(f"数据库文件不存在: {DB_PATH}")
        raise FileNotFoundError(f"数据库文件不存在: {DB_PATH}")

    logger.info(f"连接到数据库: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # 使结果可以通过列名访问
    return conn

def get_latest_kline(conn):
    """获取最新的K线数据"""
    cursor = conn.cursor()
    try:
        # 查询最新的K线数据（按open_time降序排序）
        cursor.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY open_time DESC LIMIT 1")
        row = cursor.fetchone()
        return row
    except sqlite3.Error as e:
        logger.error(f"查询数据库时出错: {e}")
        return None
    finally:
        cursor.close()

def main():
    """主函数"""
    logger.info(f"开始监控BTC {PERIOD}周期K线数据库")

    try:
        conn = connect_to_database()

        # 检查表是否存在
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'")
        if not cursor.fetchone():
            logger.error(f"表 {TABLE_NAME} 不存在")
            return
        cursor.close()

        logger.info(f"表 {TABLE_NAME} 存在，开始监控...")

        # 记录上一次的数据，用于检测变化
        last_close = None
        last_volume = None

        # 设置结束时间
        end_time = time.time() + DURATION

        while time.time() < end_time:
            # 获取最新K线数据
            kline = get_latest_kline(conn)

            if kline:
                close_price = kline['close']
                volume = kline['volume']
                open_time = kline['open_time']

                # 检查数据是否有变化
                if close_price != last_close or volume != last_volume:
                    time_str = format_timestamp(open_time)
                    logger.info(f"价格: {close_price} | 成交量: {volume}")
                    last_close = close_price
                    last_volume = volume
            else:
                logger.warning("未找到K线数据")

            # 等待下一次查询
            time.sleep(QUERY_INTERVAL)

        logger.info(f"监控完成（{DURATION}秒时间到）")

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序异常: {e}")
        logger.exception(e)  # 打印完整的异常堆栈
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("数据库连接已关闭")

if __name__ == "__main__":
    main()
