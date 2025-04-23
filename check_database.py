#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库查询脚本 - 检查K线数据下载情况
作者: Augment Agent
日期: 2025-04-22
"""

import os
import sqlite3
import datetime
from collections import defaultdict
from prettytable import PrettyTable

# 数据库路径
DB_PATH = "./data/klines.db"

def convert_timestamp_to_datetime(timestamp):
    """将Unix时间戳转换为可读时间"""
    # 币安时间戳是毫秒级的，需要除以1000转换为秒
    dt = datetime.datetime.fromtimestamp(timestamp / 1000)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def main():
    # 检查数据库是否存在
    if not os.path.exists(DB_PATH):
        print(f"错误: 数据库文件不存在于路径: {DB_PATH}")
        return

    # 连接数据库
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # 获取品种数量（从symbols表）
        cursor.execute("SELECT COUNT(*) FROM symbols")
        symbol_count = cursor.fetchone()[0]
        print(f"数据库中的品种数量: {symbol_count}")

        # 获取表数量（排除symbols表）
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name != 'symbols'")
        table_count = cursor.fetchone()[0]
        print(f"数据库中的K线表数量: {table_count}")

        # 获取所有表名（排除symbols表）
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'symbols' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]

        # 创建结果列表
        results = []

        # 遍历每个表，获取数据数量和时间范围
        for table in tables:
            # 解析表名，获取品种和周期
            parts = table.split('_')
            if len(parts) >= 3:
                symbol = parts[1]
                interval = parts[2]

                # 获取数据数量
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]

                # 获取最早和最晚的时间
                cursor.execute(f"SELECT MIN(open_time), MAX(open_time) FROM {table}")
                min_time, max_time = cursor.fetchone()

                # 转换时间戳
                start_time = "N/A"
                end_time = "N/A"

                if min_time:
                    start_time = convert_timestamp_to_datetime(min_time)

                if max_time:
                    end_time = convert_timestamp_to_datetime(max_time)

                # 添加到结果列表
                results.append({
                    'symbol': symbol,
                    'interval': interval,
                    'count': count,
                    'start_time': start_time,
                    'end_time': end_time
                })

        # 按品种和周期排序结果
        def sort_key(item):
            # 周期排序顺序
            interval_order = {
                '1m': 1,
                '5m': 2,
                '30m': 3,
                '4h': 4,
                '1d': 5,
                '1w': 6
            }
            return (item['symbol'], interval_order.get(item['interval'], 99))

        sorted_results = sorted(results, key=sort_key)

        # 输出结果表格
        print("\n数据库中的K线数据详情:")
        table = PrettyTable()
        table.field_names = ["品种", "周期", "数据数量", "开始时间", "结束时间"]
        for result in sorted_results:
            table.add_row([
                result['symbol'],
                result['interval'],
                result['count'],
                result['start_time'],
                result['end_time']
            ])
        print(table)

        # 按周期统计
        interval_stats = defaultdict(lambda: {'table_count': 0, 'total_records': 0})
        for result in sorted_results:
            interval = result['interval']
            interval_stats[interval]['table_count'] += 1
            interval_stats[interval]['total_records'] += result['count']

        print("\n按周期统计:")
        interval_table = PrettyTable()
        interval_table.field_names = ["周期", "表数量", "总记录数"]
        
        # 按周期顺序排序
        interval_order = ['1m', '5m', '30m', '4h', '1d', '1w']
        for interval in interval_order:
            if interval in interval_stats:
                stats = interval_stats[interval]
                interval_table.add_row([
                    interval,
                    stats['table_count'],
                    stats['total_records']
                ])
        print(interval_table)

        # 按品种统计
        symbol_stats = defaultdict(lambda: {'table_count': 0, 'total_records': 0})
        for result in sorted_results:
            symbol = result['symbol']
            symbol_stats[symbol]['table_count'] += 1
            symbol_stats[symbol]['total_records'] += result['count']

        print("\n按品种统计 (前20个):")
        symbol_table = PrettyTable()
        symbol_table.field_names = ["品种", "表数量", "总记录数"]
        
        # 按总记录数排序，取前20个
        sorted_symbols = sorted(symbol_stats.items(), key=lambda x: x[1]['total_records'], reverse=True)[:20]
        for symbol, stats in sorted_symbols:
            symbol_table.add_row([
                symbol,
                stats['table_count'],
                stats['total_records']
            ])
        print(symbol_table)

        # 总记录数
        total_records = sum(result['count'] for result in results)
        print(f"\n数据库中的总记录数: {total_records}")

    finally:
        # 关闭数据库连接
        conn.close()

if __name__ == "__main__":
    main()
