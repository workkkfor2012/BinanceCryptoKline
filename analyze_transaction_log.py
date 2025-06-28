#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
from collections import defaultdict
from datetime import datetime

def find_latest_transaction_log():
    """查找最新的交易日志文件"""
    log_dir = "logs/transaction_log"
    if not os.path.exists(log_dir):
        return None
    
    log_files = [f for f in os.listdir(log_dir) if f.startswith("transaction_") and f.endswith(".log")]
    if not log_files:
        return None
    
    # 按文件名排序，最新的在最后
    log_files.sort()
    return os.path.join(log_dir, log_files[-1])

def process_transaction_log(log_file_path):
    """处理交易日志文件"""
    transactions = defaultdict(list)
    
    print(f"正在分析日志文件: {log_file_path}")
    
    with open(log_file_path, 'r', encoding='utf-8') as f:
        line_count = 0
        for line in f:
            line_count += 1
            try:
                log_entry = json.loads(line.strip())
                # 确保是事务日志并且有ID
                if log_entry.get("log_type") == "transaction" and "transaction_id" in log_entry.get("fields", {}):
                    tx_id = log_entry["fields"]["transaction_id"]
                    transactions[tx_id].append(log_entry)
            except json.JSONDecodeError:
                continue # 忽略格式不正确的行
    
    print(f"总共处理了 {line_count} 行日志")
    
    # 按时间戳对每个事务内部的事件进行排序
    for tx_id in transactions:
        transactions[tx_id].sort(key=lambda x: x['timestamp'])

    return transactions

def analyze_transactions(transactions):
    """分析事务数据"""
    print(f"\n=== 交易日志分析结果 ===")
    print(f"总共找到 {len(transactions)} 个事务")
    
    # 统计事件类型
    event_types = defaultdict(int)
    symbols = set()
    intervals = set()
    
    for tx_id, events in transactions.items():
        for event in events:
            fields = event['fields']
            event_types[fields.get('event_name', 'unknown')] += 1
            if 'symbol' in fields:
                symbols.add(fields['symbol'])
            if 'interval' in fields:
                intervals.add(fields['interval'])
    
    print(f"\n事件类型统计:")
    for event_type, count in sorted(event_types.items()):
        print(f"  {event_type}: {count} 次")
    
    print(f"\n涉及的交易对数量: {len(symbols)}")
    print(f"涉及的时间间隔: {sorted(intervals)}")
    
    # 显示前几个事务的详细信息
    print(f"\n=== 前5个事务的详细故事线 ===")
    count = 0
    for tx_id in sorted(transactions.keys()):
        if count >= 5:
            break
        
        print(f"\n--- 事务 ID: {tx_id} ---")
        events = transactions[tx_id]
        print(f"事件数量: {len(events)}")
        
        for event in events:
            fields = event['fields']
            timestamp = event['timestamp']
            # 简化时间戳显示
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                time_str = dt.strftime('%H:%M:%S.%f')[:-3]  # 显示到毫秒
            except:
                time_str = timestamp
            
            symbol = fields.get('symbol', 'N/A')
            interval = fields.get('interval', 'N/A')
            event_name = fields.get('event_name', 'unknown')
            reason = fields.get('reason', '')
            
            print(f"  {time_str} - {event_name}")
            if symbol != 'N/A':
                print(f"    符号: {symbol}")
            if interval != 'N/A':
                print(f"    间隔: {interval}")
            if reason:
                print(f"    原因: {reason}")
        
        count += 1

def main():
    # 查找最新的日志文件
    latest_log = find_latest_transaction_log()
    if not latest_log:
        print("未找到交易日志文件")
        return
    
    # 处理日志
    transactions = process_transaction_log(latest_log)
    
    if not transactions:
        print("未找到任何事务数据")
        return
    
    # 分析结果
    analyze_transactions(transactions)

if __name__ == "__main__":
    main()
