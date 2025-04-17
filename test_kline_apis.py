import requests
import json
import time

# 代理设置 (SOCKS5 代理需要 pip install requests[socks] 并将 http:// 改为 socks5://)
PROXIES = {
    'http': 'http://127.0.0.1:1080',
    'https': 'http://127.0.0.1:1080',
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
LIMIT = 1

def fetch_data(url, params, use_proxy=False):
    """通用数据获取函数"""
    print(f"请求 URL: {url}")
    print(f"请求参数: {params}")
    print(f"使用代理: {use_proxy}")
    try:
        proxies_to_use = PROXIES if use_proxy else None
        response = requests.get(url, params=params, proxies=proxies_to_use, headers=HEADERS, timeout=15) # 增加超时时间
        print(f"状态码: {response.status_code}")
        response.raise_for_status() # 如果请求失败则抛出异常
        result_json = response.json()
        print(f"原始响应: {response.text[:200]}...") # 打印部分原始响应
        return result_json
    except requests.exceptions.ProxyError as e:
        return f"代理错误: {e}. 请确保代理服务器 127.0.0.1:1080 正在运行并可访问。"
    except requests.exceptions.Timeout:
        return "请求超时。网络问题或目标服务器响应慢。"
    except requests.exceptions.RequestException as e:
        return f"请求错误: {e}"
    except json.JSONDecodeError:
        return f"响应不是有效的 JSON 格式。原始响应: {response.text[:200]}..."
    except Exception as e:
        return f"发生未知错误: {e}"

def test_method_1():
    """测试方法 1: https://fapi.binance.com/fapi/v1/klines"""
    print("\n--- 测试 1: fapi.binance.com /fapi/v1/klines (需要代理) ---")
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": LIMIT,
        # 注意：原始 URL 中的 timeZone=8:00 对此 API 无效，因此移除
    }
    result = fetch_data(url, params, use_proxy=True)
    print(f"结果: {json.dumps(result, indent=2)}")
    return result

def test_method_2():
    """测试方法 2: https://data-api.binance.vision/api/v3/klines"""
    print("\n--- 测试 2: data-api.binance.vision /api/v3/klines (不需要代理) ---")
    # 注意：此接口通常用于现货数据，结果可能与合约不同或失败
    url = "https://data-api.binance.vision/api/v3/klines"
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": LIMIT
    }
    result = fetch_data(url, params, use_proxy=False)
    print(f"结果: {json.dumps(result, indent=2)}")
    return result

def test_method_3():
    """测试方法 3: https://data-api.binance.vision/api/v3/uiKlines"""
    print("\n--- 测试 3: data-api.binance.vision /api/v3/uiKlines (不需要代理) ---")
    # 注意：此接口通常用于现货数据，结果可能与合约不同或失败
    url = "https://data-api.binance.vision/api/v3/uiKlines"
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": LIMIT
    }
    result = fetch_data(url, params, use_proxy=False)
    print(f"结果: {json.dumps(result, indent=2)}")
    return result

def test_method_4():
    """测试方法 4: https://fapi.binance.com/fapi/v1/continuousKlines"""
    print("\n--- 测试 4: fapi.binance.com /fapi/v1/continuousKlines (需要代理) ---")
    url = "https://fapi.binance.com/fapi/v1/continuousKlines"
    params = {
        "pair": SYMBOL, # 注意这里是 pair
        "contractType": "PERPETUAL", # 永续合约
        "interval": INTERVAL,
        "limit": LIMIT
    }
    result = fetch_data(url, params, use_proxy=True)
    print(f"结果: {json.dumps(result, indent=2)}")
    return result

if __name__ == "__main__":
    print(f"开始测试下载最新的 {SYMBOL} {INTERVAL} K线 (limit={LIMIT})...")
    proxy_status = '启用 (127.0.0.1:1080)' if PROXIES else '禁用'
    print(f"代理设置: {proxy_status}. 需要代理的接口将尝试使用此设置。")
    print("-" * 30)

    results = {}
    results['method_1'] = test_method_1()
    print("-" * 30)
    time.sleep(1) # 稍微暂停，避免请求过于频繁

    results['method_2'] = test_method_2()
    print("-" * 30)
    time.sleep(1)

    results['method_3'] = test_method_3()
    print("-" * 30)
    time.sleep(1)

    results['method_4'] = test_method_4()
    print("-" * 30)

    print("\n--- 测试完成 ---")
    print("请检查上面每个测试的输出结果进行对比。")
    print("注意：方法 2 和 3 使用的 data-api.binance.vision/api/v3/* 通常是现货接口，可能无法正确获取合约数据。")