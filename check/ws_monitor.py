#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BTC K线WebSocket监控脚本
- 通过WebSocket订阅币安的K线
- 提取并打印收盘价和成交量到控制台
- 从环境变量中读取周期
"""

import asyncio
import json
from datetime import datetime
import logging
import os
import aiohttp
import time

# 从环境变量中读取周期，默认为4h
PERIOD = os.environ.get("BTC_PERIOD", "4h")

# 从环境变量中读取监控持续时间（秒），默认为60秒
DURATION = int(os.environ.get("BTC_DURATION", "60"))

# 配置日志
log_file = f"btc_{PERIOD}_monitor.log"
logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - [WS-{PERIOD}] - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(f"ws_monitor_{PERIOD}")

# WebSocket配置
BINANCE_WS_URL = "wss://fstream.binance.com/stream"
PROXY = "http://127.0.0.1:1080"  # 代理设置
SYMBOL = "BTCUSDT"
INTERVAL = PERIOD
STREAM_NAME = f"{SYMBOL.lower()}_perpetual@continuousKline_{INTERVAL}"

# 全局变量
last_ws_kline = None

def format_timestamp(timestamp_ms):
    """将毫秒时间戳格式化为可读时间"""
    dt = datetime.fromtimestamp(timestamp_ms / 1000)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

async def connect_websocket():
    """连接到币安WebSocket并订阅K线数据，使用aiohttp库"""
    global last_ws_kline

    # 设置代理环境变量
    os.environ['http_proxy'] = PROXY
    os.environ['https_proxy'] = PROXY

    logger.info(f"开始监控BTC {PERIOD}周期K线WebSocket")

    # 使用组合流URL格式
    ws_url = f"{BINANCE_WS_URL}?streams={STREAM_NAME}"

    # 设置结束时间
    end_time = time.time() + DURATION

    while time.time() < end_time:
        try:
            # 创建aiohttp会话，配置代理
            session = aiohttp.ClientSession()

            # 连接到WebSocket
            logger.info(f"连接到WebSocket...")

            async with session.ws_connect(ws_url, proxy=PROXY, timeout=30) as ws:
                logger.info("WebSocket连接成功")

                # 接收消息
                message_count = 0
                async for msg in ws:
                    try:
                        # 处理不同类型的消息
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            message_count += 1
                            response = msg.data

                            # 解析JSON
                            data = json.loads(response)

                            # 处理K线数据
                            if "data" in data and "k" in data["data"]:
                                kline_data = data["data"]["k"]

                                # 记录K线数据的详细信息
                                close_price = kline_data["c"]
                                volume = kline_data["v"]

                                logger.info(f"价格: {close_price} | 成交量: {volume}")

                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            logger.info("WebSocket连接已关闭")
                            break

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"WebSocket连接错误: {msg.data}")
                            break

                        # 检查是否超过了监控时间
                        if time.time() >= end_time:
                            logger.info(f"监控完成（{DURATION}秒时间到）")
                            break

                    except json.JSONDecodeError as e:
                        logger.error(f"解析WebSocket消息失败: {e}")
                    except Exception as e:
                        logger.error(f"处理WebSocket消息时出错: {e}")
                        logger.exception(e)  # 打印完整的异常堆栈

                logger.info("WebSocket连接循环结束")

        except aiohttp.ClientError as e:
            logger.error(f"aiohttp客户端错误: {e}")
        except asyncio.TimeoutError:
            logger.error("WebSocket连接超时")
        except Exception as e:
            logger.error(f"WebSocket连接出错: {e}")
        finally:
            # 确保会话被关闭
            if 'session' in locals() and not session.closed:
                await session.close()
                logger.info("aiohttp会话已关闭")

            # 如果时间未到，等待一下再重连
            if time.time() < end_time:
                wait_time = min(5, end_time - time.time())
                if wait_time > 0:
                    logger.info(f"{wait_time:.1f}秒后重新连接...")
                    await asyncio.sleep(wait_time)
            else:
                break

    logger.info("WebSocket监控完成")

async def main():
    """主函数"""
    # 创建WebSocket连接任务
    ws_task = asyncio.create_task(connect_websocket())

    # 等待任务完成
    await ws_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        logger.exception(e)  # 打印完整的异常堆栈
