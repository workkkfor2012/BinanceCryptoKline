import json
import os
from collections import defaultdict
from datetime import datetime

def find_latest_transaction_log():
    """查找最新的交易日志文件"""
    log_dir = "logs/transaction_log"
    if not os.path.exists(log_dir):
        return None

    # 只查找原始日志文件，排除已分组的文件
    log_files = [f for f in os.listdir(log_dir) if f.startswith("transaction_") and f.endswith(".log") and "_grouped" not in f]
    if not log_files:
        return None

    # 按文件名排序，最新的在最后
    log_files.sort()
    return os.path.join(log_dir, log_files[-1])

def process_transaction_log(log_file_path):
    transactions = defaultdict(list)

    with open(log_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                log_entry = json.loads(line.strip())
                # 确保是事务日志并且有ID
                if log_entry.get("log_type") == "transaction" and "transaction_id" in log_entry.get("fields", {}):
                    tx_id = log_entry["fields"]["transaction_id"]
                    transactions[tx_id].append(log_entry)
            except json.JSONDecodeError:
                continue # 忽略格式不正确的行

    # 按时间戳对每个事务内部的事件进行排序
    for tx_id in transactions:
        transactions[tx_id].sort(key=lambda x: x['timestamp'])

    return transactions

def generate_grouped_log_file(transactions, output_file_path):
    """生成按transaction_id分组的新日志文件"""
    with open(output_file_path, 'w', encoding='utf-8') as f:
        # 按transaction_id排序
        for tx_id in sorted(transactions.keys()):
            # 写入分割线
            f.write(f"{'='*80}\n")

            # 写入该事务的所有日志条目（只输出原始JSON）
            for log_entry in transactions[tx_id]:
                f.write(f"{json.dumps(log_entry, ensure_ascii=False)}\n")

            f.write(f"{'='*80}\n")  # 事务结束分割线

if __name__ == "__main__":
    # 查找最新的日志文件
    latest_log = find_latest_transaction_log()
    if not latest_log:
        print("未找到交易日志文件")
        exit(1)

    print(f"正在处理日志文件: {latest_log}")

    # 处理日志
    all_transactions = process_transaction_log(latest_log)

    if not all_transactions:
        print("未找到任何事务数据")
        exit(1)

    # 生成输出文件名
    base_name = os.path.basename(latest_log)
    name_without_ext = os.path.splitext(base_name)[0]
    output_file = f"logs/transaction_log/{name_without_ext}_grouped.log"

    # 生成分组后的日志文件
    generate_grouped_log_file(all_transactions, output_file)

    print(f"已生成分组日志文件: {output_file}")
    print(f"总共处理了 {len(all_transactions)} 个事务")