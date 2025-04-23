#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库查询脚本 - 检查K线数据下载情况（拆分显示）
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

        # 创建结果字典，按品种组织数据
        symbol_data = defaultdict(dict)

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

                # 添加到结果字典
                if symbol not in symbol_data:
                    symbol_data[symbol] = {}
                
                symbol_data[symbol][interval] = {
                    'count': count,
                    'start_time': start_time,
                    'end_time': end_time
                }

        # 输出结果表格（数据数量）
        print("\n数据库中的K线数据数量:")
        count_table = PrettyTable()
        
        # 设置表头
        headers = ["品种"]
        intervals = ["1m", "5m", "30m", "4h", "1d", "1w"]
        for interval in intervals:
            headers.append(f"{interval}数量")
        
        count_table.field_names = headers
        
        # 按品种名称排序
        for symbol in sorted(symbol_data.keys()):
            row = [symbol]
            for interval in intervals:
                if interval in symbol_data[symbol]:
                    row.append(symbol_data[symbol][interval]['count'])
                else:
                    row.append("N/A")
            count_table.add_row(row)
        
        # 设置表格样式
        count_table.align = "l"  # 左对齐
        
        print(count_table)

        # 输出结果表格（时间范围）
        print("\n数据库中的K线数据时间范围:")
        time_table = PrettyTable()
        
        # 设置表头
        headers = ["品种"]
        for interval in intervals:
            headers.append(f"{interval}开始")
            headers.append(f"{interval}结束")
        
        time_table.field_names = headers
        
        # 按品种名称排序
        for symbol in sorted(symbol_data.keys()):
            row = [symbol]
            for interval in intervals:
                if interval in symbol_data[symbol]:
                    row.append(symbol_data[symbol][interval]['start_time'])
                    row.append(symbol_data[symbol][interval]['end_time'])
                else:
                    row.extend(["N/A", "N/A"])
            time_table.add_row(row)
        
        # 设置表格样式
        time_table.align = "l"  # 左对齐
        
        print(time_table)

        # 按周期统计
        interval_stats = defaultdict(lambda: {'table_count': 0, 'total_records': 0})
        for symbol_intervals in symbol_data.values():
            for interval, data in symbol_intervals.items():
                interval_stats[interval]['table_count'] += 1
                interval_stats[interval]['total_records'] += data['count']

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

        # 总记录数
        total_records = sum(stats['total_records'] for stats in interval_stats.values())
        print(f"\n数据库中的总记录数: {total_records}")

    finally:
        # 关闭数据库连接
        conn.close()

if __name__ == "__main__":
    main()
