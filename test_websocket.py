import websocket
import threading
import time
import json
import ssl
import socks
import socket

# 配置SOCKS5代理
SOCKS5_PROXY = "127.0.0.1"
SOCKS5_PORT = 1080

# 设置全局代理
socks.set_default_proxy(socks.SOCKS5, SOCKS5_PROXY, SOCKS5_PORT)
socket.socket = socks.socksocket

# WebSocket配置
STREAM_NAME = "btcusdt_perpetual@continuousKline_1m"
WEBSOCKET_URL = f"wss://fstream.binance.com/ws/{STREAM_NAME}"

def on_message(ws, message):
    """处理收到的K线消息"""
    print(f"--- 收到K线更新 ({time.strftime('%H:%M:%S')}) ---")
    try:
        data = json.loads(message)
        print(json.dumps(data, indent=2))
    except json.JSONDecodeError:
        print(f"原始消息: {message}")

def on_error(ws, error):
    """处理错误"""
    print(f"--- 连接错误 ---")
    print(f"错误类型: {type(error)}")
    print(error)

def on_close(ws, close_status_code, close_msg):
    """连接关闭回调"""
    print(f"--- 连接关闭 ---")
    print(f"状态码: {close_status_code}, 原因: {close_msg}")

def on_open(ws):
    """连接建立后自动订阅"""
    print("--- 连接已建立，正在订阅K线数据 ---")
    def run():
        time.sleep(1)  # 等待连接稳定
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": [STREAM_NAME],
            "id": 1
        }
        ws.send(json.dumps(subscribe_msg))
        print("订阅请求已发送")

    threading.Thread(target=run).start()

if __name__ == "__main__":
    print(f"正在通过SOCKS5代理 {SOCKS5_PROXY}:{SOCKS5_PORT} 连接Binance WebSocket")
    
    # 创建WebSocket连接
    ws = websocket.WebSocketApp(WEBSOCKET_URL,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    
    # 运行WebSocket
    try:
        ws.run_forever(
            sslopt={"cert_reqs": ssl.CERT_NONE},
            ping_interval=30,
            ping_timeout=10
        )
    except KeyboardInterrupt:
        print("\n手动终止连接")
    except Exception as e:
        print(f"发生异常: {e}")
    finally:
        print("程序结束")