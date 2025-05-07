#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试币安服务器时间API的响应时间和网络延迟，
并计算最佳的K线请求时间点
"""

import requests
import time
import statistics
import datetime
import argparse
import sys
import json
import threading
import os
import glob
from typing import List, Dict, Any, Tuple, Optional

# 币安API端点
BINANCE_API_URL = "https://fapi.binance.com/fapi/v1/time"
BINANCE_KLINE_API_URL = "https://fapi.binance.com/fapi/v1/klines"

def get_server_time(proxy: str = None) -> Tuple[float, Dict[str, Any]]:
    """
    获取币安服务器时间，并测量响应时间

    Args:
        proxy: 代理服务器地址，格式为 "http://host:port"

    Returns:
        响应时间(秒)和响应内容的元组
    """
    proxies = None
    if proxy:
        proxies = {
            "http": proxy,
            "https": proxy
        }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    start_time = time.time()
    response = requests.get(BINANCE_API_URL, headers=headers, proxies=proxies)
    end_time = time.time()

    response_time = end_time - start_time

    if response.status_code == 200:
        return response_time, response.json()
    else:
        raise Exception(f"API请求失败: {response.status_code} - {response.text}")

def format_timestamp(timestamp_ms: int) -> str:
    """
    将毫秒时间戳格式化为可读的日期时间字符串

    Args:
        timestamp_ms: 毫秒时间戳

    Returns:
        格式化的日期时间字符串
    """
    dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def test_server_time(count: int = 10, interval: float = 1.0, proxy: str = None) -> None:
    """
    测试币安服务器时间API的响应时间

    Args:
        count: 测试次数
        interval: 测试间隔(秒)
        proxy: 代理服务器地址
    """
    print(f"开始测试币安服务器时间API响应时间 (测试次数: {count}, 间隔: {interval}秒)")
    if proxy:
        print(f"使用代理: {proxy}")

    response_times = []
    time_diffs = []

    for i in range(count):
        try:
            # 获取本地时间
            local_time_ms = int(time.time() * 1000)

            # 获取服务器时间
            response_time, response_data = get_server_time(proxy)
            server_time_ms = response_data["serverTime"]

            # 计算时间差
            time_diff = server_time_ms - local_time_ms

            # 记录结果
            response_times.append(response_time)
            time_diffs.append(time_diff)

            # 打印结果
            print(f"测试 {i+1}/{count}:")
            print(f"  响应时间: {response_time*1000:.2f}毫秒")
            print(f"  本地时间: {format_timestamp(local_time_ms)} ({local_time_ms})")
            print(f"  服务器时间: {format_timestamp(server_time_ms)} ({server_time_ms})")
            print(f"  时间差: {time_diff}毫秒")
            print()

            # 等待指定间隔
            if i < count - 1:
                time.sleep(interval)

        except Exception as e:
            print(f"测试 {i+1}/{count} 失败: {e}")
            print()

    # 计算统计信息
    if response_times:
        avg_response_time = statistics.mean(response_times) * 1000  # 转换为毫秒
        min_response_time = min(response_times) * 1000
        max_response_time = max(response_times) * 1000

        avg_time_diff = statistics.mean(time_diffs)
        min_time_diff = min(time_diffs)
        max_time_diff = max(time_diffs)
        std_time_diff = statistics.stdev(time_diffs) if len(time_diffs) > 1 else 0

        print("统计结果:")
        print(f"  响应时间 (毫秒): 平均={avg_response_time:.2f}, 最小={min_response_time:.2f}, 最大={max_response_time:.2f}")
        print(f"  时间差 (毫秒): 平均={avg_time_diff:.2f}, 最小={min_time_diff}, 最大={max_time_diff}, 标准差={std_time_diff:.2f}")

        # 保存结果到文件
        result = {
            "timestamp": int(time.time()),
            "datetime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "test_count": count,
            "test_interval": interval,
            "proxy": proxy,
            "response_time_ms": {
                "average": avg_response_time,
                "min": min_response_time,
                "max": max_response_time
            },
            "time_diff_ms": {
                "average": avg_time_diff,
                "min": min_time_diff,
                "max": max_time_diff,
                "std_dev": std_time_diff
            },
            "raw_data": [
                {
                    "response_time_ms": rt * 1000,
                    "time_diff_ms": td
                } for rt, td in zip(response_times, time_diffs)
            ]
        }

        filename = f"binance_time_test_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, "w") as f:
            json.dump(result, f, indent=2)

        print(f"测试结果已保存到文件: {filename}")

def get_kline(symbol: str, interval: str, limit: int = 1, proxy: str = None) -> Tuple[float, List[Dict]]:
    """
    获取K线数据，并测量响应时间

    Args:
        symbol: 交易对，如 "BTCUSDT"
        interval: K线周期，如 "1m"
        limit: 获取的K线数量
        proxy: 代理服务器地址

    Returns:
        响应时间(秒)和K线数据的元组
    """
    proxies = None
    if proxy:
        proxies = {
            "http": proxy,
            "https": proxy
        }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    start_time = time.time()
    response = requests.get(BINANCE_KLINE_API_URL, params=params, headers=headers, proxies=proxies)
    end_time = time.time()

    response_time = end_time - start_time

    if response.status_code == 200:
        return response_time, response.json()
    else:
        raise Exception(f"K线API请求失败: {response.status_code} - {response.text}")

def calculate_optimal_request_time(avg_network_delay: float, time_diff: float, safety_margin: float = 30) -> datetime.datetime:
    """
    计算最佳的K线请求时间点

    Args:
        avg_network_delay: 平均网络延迟(毫秒)
        time_diff: 本地时间与服务器时间的差值(毫秒)
        safety_margin: 安全边际(毫秒)，提前发送请求的额外时间

    Returns:
        下一分钟的最佳请求时间点
    """
    # 获取当前本地时间
    now = datetime.datetime.now()

    # 计算下一分钟的开始时间（服务器时间）
    next_minute = now.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)

    # 计算请求应该到达服务器的时间点
    # 我们希望请求在下一分钟的第0毫秒到达服务器
    target_arrival_time = next_minute

    # 计算本地应该发送请求的时间点
    # 公式: 目标到达时间 - 网络延迟 - 安全边际
    # 注意：时间差已经包含在计算中，因为我们是基于本地时间计算的下一分钟开始
    optimal_time = target_arrival_time - datetime.timedelta(milliseconds=avg_network_delay + safety_margin)

    # 如果时间差为负（本地时间比服务器时间早），则需要更早发送请求
    if time_diff < 0:
        optimal_time = optimal_time - datetime.timedelta(milliseconds=abs(time_diff))
    # 如果时间差为正（本地时间比服务器时间晚），则需要更晚发送请求
    else:
        optimal_time = optimal_time + datetime.timedelta(milliseconds=time_diff)

    return optimal_time

def test_optimal_kline_request(avg_network_delay: float, time_diff: float, symbol: str = "BTCUSDT",
                              interval: str = "1m", proxy: str = None, safety_margin: float = 30) -> None:
    """
    测试在最佳时间点请求K线数据

    Args:
        avg_network_delay: 平均网络延迟(毫秒)
        time_diff: 本地时间与服务器时间的差值(毫秒)
        symbol: 交易对
        interval: K线周期
        proxy: 代理服务器地址
        safety_margin: 安全边际(毫秒)
    """
    # 获取当前本地时间
    now = datetime.datetime.now()

    # 计算下一分钟的开始时间
    next_minute = now.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)

    # 计算最佳请求时间点
    optimal_time = calculate_optimal_request_time(avg_network_delay, time_diff, safety_margin)

    print(f"\n测试最佳K线请求时间点:")
    print(f"  交易对: {symbol}")
    print(f"  周期: {interval}")
    print(f"  当前本地时间: {now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    print(f"  下一分钟开始: {next_minute.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    print(f"  平均网络延迟: {avg_network_delay:.2f}毫秒")
    print(f"  时间差: {time_diff:.2f}毫秒 ({('本地时间比服务器时间早' if time_diff < 0 else '本地时间比服务器时间晚')})")
    print(f"  安全边际: {safety_margin}毫秒")
    print(f"  计算公式: 下一分钟开始时间 - 网络延迟({avg_network_delay:.2f}ms) - 安全边际({safety_margin}ms) {('-' if time_diff < 0 else '+')} 时间差({abs(time_diff):.2f}ms)")
    print(f"  最佳请求时间点: {optimal_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    print(f"  距离下一分钟开始还有: {((next_minute - optimal_time).total_seconds() * 1000):.2f}毫秒")

    # 计算需要等待的时间
    wait_time = (optimal_time - datetime.datetime.now()).total_seconds()

    if wait_time > 0:
        print(f"  等待 {wait_time:.2f}秒...")
        time.sleep(wait_time)
    else:
        print(f"  最佳请求时间已过，立即发送请求...")

    # 发送请求
    try:
        request_time = datetime.datetime.now()
        response_time, klines = get_kline(symbol, interval, 1, proxy)

        # 解析K线数据
        if klines and len(klines) > 0:
            kline = klines[0]
            open_time = int(kline[0])
            open_time_str = format_timestamp(open_time)
            close_time = int(kline[6])
            close_time_str = format_timestamp(close_time)

            print(f"  请求发送时间: {request_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            print(f"  响应时间: {response_time*1000:.2f}毫秒")
            print(f"  获取到的K线:")
            print(f"    开盘时间: {open_time_str} ({open_time})")
            print(f"    收盘时间: {close_time_str} ({close_time})")

            # 检查是否获取到了最新的K线
            current_minute = datetime.datetime.now().replace(second=0, microsecond=0)
            kline_minute = datetime.datetime.fromtimestamp(open_time / 1000).replace(second=0, microsecond=0)

            if kline_minute >= current_minute:
                print(f"  成功: 获取到了最新的K线数据!")
            else:
                print(f"  失败: 获取到的不是最新的K线数据。")
                print(f"    当前分钟: {current_minute.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"    K线分钟: {kline_minute.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"  未获取到K线数据")
    except Exception as e:
        print(f"  请求失败: {e}")

def main():
    parser = argparse.ArgumentParser(description="测试币安服务器时间API的响应时间和网络延迟，并计算最佳的K线请求时间点")
    parser.add_argument("-c", "--count", type=int, default=10, help="测试次数 (默认: 10)")
    parser.add_argument("-i", "--interval", type=float, default=1.0, help="测试间隔(秒) (默认: 1.0)")
    parser.add_argument("-p", "--proxy", type=str, default="http://127.0.0.1:1080", help="代理服务器地址 (默认: http://127.0.0.1:1080)")
    parser.add_argument("-s", "--symbol", type=str, default="BTCUSDT", help="交易对 (默认: BTCUSDT)")
    parser.add_argument("-t", "--timeframe", type=str, default="1m", help="K线周期 (默认: 1m)")
    parser.add_argument("-m", "--margin", type=float, default=30, help="安全边际(毫秒) (默认: 30)")
    parser.add_argument("--test-kline", action="store_true", help="测试最佳K线请求时间点")

    args = parser.parse_args()

    try:
        # 测试服务器时间
        test_server_time(args.count, args.interval, args.proxy)

        # 如果指定了测试K线请求
        if args.test_kline:
            # 获取最近一次测试的结果
            try:
                # 查找最新的测试结果文件
                result_files = glob.glob("binance_time_test_*.json")
                if result_files:
                    latest_file = max(result_files, key=os.path.getctime)
                    with open(latest_file, "r") as f:
                        result = json.load(f)

                    avg_network_delay = result["response_time_ms"]["average"]
                    avg_time_diff = result["time_diff_ms"]["average"]

                    # 测试最佳K线请求时间点
                    test_optimal_kline_request(
                        avg_network_delay,
                        avg_time_diff,
                        args.symbol,
                        args.timeframe,
                        args.proxy,
                        args.margin
                    )
                else:
                    print("未找到测试结果文件，无法测试最佳K线请求时间点")
            except Exception as e:
                print(f"测试最佳K线请求时间点失败: {e}")
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"测试失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
