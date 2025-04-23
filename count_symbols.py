#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
查询数据库中的品种数量
作者: Augment Agent
日期: 2025-04-22
"""

import os
import sqlite3

# 数据库路径
DB_PATH = "./data/klines.db"

def main():
    # 检查数据库是否存在
    if not os.path.exists(DB_PATH):
        print(f"错误: 数据库文件不存在于路径: {DB_PATH}")
        return

    # 连接数据库
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # 方法1: 从symbols表获取品种数量
        cursor.execute("SELECT COUNT(*) FROM symbols")
        symbol_count_from_table = cursor.fetchone()[0]
        print(f"从symbols表获取的品种数量: {symbol_count_from_table}")

        # 方法2: 从K线表名称获取唯一品种数量
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'k_%'")
        tables = [row[0] for row in cursor.fetchall()]

        # 从表名中提取品种名称
        symbols = set()
        for table in tables:
            parts = table.split('_')
            if len(parts) >= 3:
                symbol = parts[1]
                symbols.add(symbol)

        print(f"从K线表名称获取的唯一品种数量: {len(symbols)}")

        # 方法3: 计算每个周期的表数量
        intervals = ['1m', '5m', '30m', '4h', '1d', '1w']
        for interval in intervals:
            cursor.execute(f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'k_%_{interval}'")
            table_count = cursor.fetchone()[0]
            print(f"{interval}周期的表数量: {table_count}")

    finally:
        # 关闭数据库连接
        conn.close()

if __name__ == "__main__":
    main()
