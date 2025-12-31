# VN.Py 信号接收方案完整指南

## 📋 目录
1. [RabbitMQ集成方案](#rabbitmq集成方案)
2. [VN.Py支持的信号接收方式](#vnpy支持的信号接收方式)
3. [方案对比与选择](#方案对比与选择)
4. [实现示例](#实现示例)

---

## 🐰 RabbitMQ集成方案

### 架构设计

```
┌─────────────────┐
│  DolphinDB      │
│  (计算信号)      │
└────────┬────────┘
         │
         │ 发布信号消息
         ▼
┌─────────────────┐
│   RabbitMQ      │
│   (消息队列)     │
│   Exchange: signals│
│   Queue: vnpy_signals│
└────────┬────────┘
         │
         │ 订阅消费消息
         ▼
┌─────────────────┐
│  VN.Py          │
│  SignalReceiver │
│  (信号接收引擎)  │
└────────┬────────┘
         │
         │ 转换为OrderRequest
         ▼
┌─────────────────┐
│  MainEngine     │
│  (发送订单)      │
└─────────────────┘
```

### 实现步骤

#### 1. 创建信号接收App模块

**目录结构**：
```
vnpy/app/signal_receiver/
├── __init__.py
├── engine.py          # 信号接收引擎
├── rabbitmq_client.py # RabbitMQ客户端封装
└── ui/
    └── widget.py      # UI界面（可选）
```

#### 2. 信号消息格式定义

```python
# 信号消息格式（JSON）
{
    "strategy_id": "strategy_001",      # 策略ID
    "symbol": "rb2310",                  # 合约代码
    "exchange": "SHFE",                  # 交易所
    "direction": "LONG",                 # 方向: LONG/SHORT
    "offset": "OPEN",                    # 开平: OPEN/CLOSE/CLOSETODAY
    "price": 3500.0,                     # 价格
    "volume": 1,                         # 数量
    "order_type": "LIMIT",               # 订单类型: LIMIT/MARKET
    "gateway_name": "CTP",               # 网关名称
    "timestamp": "2024-01-01 10:00:00", # 时间戳
    "remark": ""                         # 备注
}
```

#### 3. RabbitMQ客户端封装

```python
# vnpy/app/signal_receiver/rabbitmq_client.py
import pika
import json
import logging
from typing import Callable, Optional
from threading import Thread

class RabbitMQClient:
    """RabbitMQ客户端封装"""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        exchange: str = "signals",
        queue: str = "vnpy_signals",
        routing_key: str = "signal.#"
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.exchange = exchange
        self.queue = queue
        self.routing_key = routing_key
        
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.callback: Optional[Callable] = None
        self.active = False
        self.thread: Optional[Thread] = None
        
    def connect(self):
        """连接RabbitMQ服务器"""
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials
        )
        
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        
        # 声明Exchange
        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type='topic',
            durable=True
        )
        
        # 声明Queue
        self.channel.queue_declare(
            queue=self.queue,
            durable=True
        )
        
        # 绑定Queue到Exchange
        self.channel.queue_bind(
            exchange=self.exchange,
            queue=self.queue,
            routing_key=self.routing_key
        )
        
    def start_consuming(self, callback: Callable):
        """开始消费消息"""
        self.callback = callback
        self.active = True
        
        def consume():
            try:
                while self.active:
                    method_frame, header_frame, body = self.channel.basic_get(
                        queue=self.queue,
                        auto_ack=False
                    )
                    
                    if method_frame:
                        try:
                            # 解析消息
                            message = json.loads(body.decode('utf-8'))
                            
                            # 调用回调函数
                            if self.callback:
                                self.callback(message)
                            
                            # 确认消息
                            self.channel.basic_ack(method_frame.delivery_tag)
                        except Exception as e:
                            logging.error(f"处理消息失败: {e}")
                            # 拒绝消息（不重新入队）
                            self.channel.basic_nack(
                                method_frame.delivery_tag,
                                requeue=False
                            )
                    else:
                        # 没有消息时短暂休眠
                        import time
                        time.sleep(0.1)
            except Exception as e:
                logging.error(f"消费消息异常: {e}")
        
        self.thread = Thread(target=consume, daemon=True)
        self.thread.start()
    
    def stop(self):
        """停止消费"""
        self.active = False
        if self.connection and not self.connection.is_closed:
            self.connection.close()
    
    def publish(self, message: dict, routing_key: str = None):
        """发布消息（用于测试）"""
        if not self.channel:
            self.connect()
        
        routing_key = routing_key or self.routing_key
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # 持久化消息
            )
        )
```

#### 4. 信号接收引擎实现

```python
# vnpy/app/signal_receiver/engine.py
import json
import logging
from typing import Optional, Dict, Any
from threading import Thread, Lock
from queue import Queue

from vnpy.event import EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.object import (
    OrderRequest, Direction, Offset, Exchange, OrderType
)
from vnpy.trader.constant import Direction as Dir, Offset as Off, OrderType as OT
from vnpy.trader.utility import load_json, save_json

from .rabbitmq_client import RabbitMQClient

APP_NAME = "SignalReceiver"

EVENT_SIGNAL_LOG = "eSignalLog"


class SignalReceiverEngine(BaseEngine):
    """信号接收引擎"""
    
    setting_filename = "signal_receiver_setting.json"
    
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super().__init__(main_engine, event_engine, APP_NAME)
        
        # RabbitMQ配置
        self.rabbitmq_host = "localhost"
        self.rabbitmq_port = 5672
        self.rabbitmq_username = "guest"
        self.rabbitmq_password = "guest"
        self.rabbitmq_exchange = "signals"
        self.rabbitmq_queue = "vnpy_signals"
        self.rabbitmq_routing_key = "signal.#"
        
        # RabbitMQ客户端
        self.rabbitmq_client: Optional[RabbitMQClient] = None
        
        # 信号处理队列（用于批量处理）
        self.signal_queue = Queue()
        self.batch_size = 10  # 批量处理大小
        self.batch_timeout = 0.1  # 批量处理超时（秒）
        
        # 统计信息
        self.signal_count = 0
        self.order_count = 0
        self.error_count = 0
        
        # 线程锁
        self.lock = Lock()
        
        # 加载配置
        self.load_setting()
        
        # 注册事件
        self.register_event()
    
    def load_setting(self):
        """加载配置"""
        setting = load_json(self.setting_filename)
        if setting:
            self.rabbitmq_host = setting.get("rabbitmq_host", self.rabbitmq_host)
            self.rabbitmq_port = setting.get("rabbitmq_port", self.rabbitmq_port)
            self.rabbitmq_username = setting.get("rabbitmq_username", self.rabbitmq_username)
            self.rabbitmq_password = setting.get("rabbitmq_password", self.rabbitmq_password)
            self.rabbitmq_exchange = setting.get("rabbitmq_exchange", self.rabbitmq_exchange)
            self.rabbitmq_queue = setting.get("rabbitmq_queue", self.rabbitmq_queue)
            self.rabbitmq_routing_key = setting.get("rabbitmq_routing_key", self.rabbitmq_routing_key)
            self.batch_size = setting.get("batch_size", self.batch_size)
            self.batch_timeout = setting.get("batch_timeout", self.batch_timeout)
    
    def save_setting(self):
        """保存配置"""
        setting = {
            "rabbitmq_host": self.rabbitmq_host,
            "rabbitmq_port": self.rabbitmq_port,
            "rabbitmq_username": self.rabbitmq_username,
            "rabbitmq_password": self.rabbitmq_password,
            "rabbitmq_exchange": self.rabbitmq_exchange,
            "rabbitmq_queue": self.rabbitmq_queue,
            "rabbitmq_routing_key": self.rabbitmq_routing_key,
            "batch_size": self.batch_size,
            "batch_timeout": self.batch_timeout
        }
        save_json(self.setting_filename, setting)
    
    def start(self):
        """启动信号接收"""
        try:
            # 创建RabbitMQ客户端
            self.rabbitmq_client = RabbitMQClient(
                host=self.rabbitmq_host,
                port=self.rabbitmq_port,
                username=self.rabbitmq_username,
                password=self.rabbitmq_password,
                exchange=self.rabbitmq_exchange,
                queue=self.rabbitmq_queue,
                routing_key=self.rabbitmq_routing_key
            )
            
            # 连接RabbitMQ
            self.rabbitmq_client.connect()
            
            # 开始消费消息
            self.rabbitmq_client.start_consuming(self.process_signal)
            
            # 启动批量处理线程
            self.start_batch_processor()
            
            self.write_log("信号接收引擎启动成功")
            return True
            
        except Exception as e:
            self.write_log(f"信号接收引擎启动失败: {e}")
            return False
    
    def stop(self):
        """停止信号接收"""
        if self.rabbitmq_client:
            self.rabbitmq_client.stop()
        self.write_log("信号接收引擎已停止")
    
    def process_signal(self, signal: Dict[str, Any]):
        """处理接收到的信号"""
        try:
            with self.lock:
                self.signal_count += 1
            
            # 将信号放入处理队列
            self.signal_queue.put(signal)
            
        except Exception as e:
            with self.lock:
                self.error_count += 1
            self.write_log(f"处理信号失败: {e}")
    
    def start_batch_processor(self):
        """启动批量处理线程"""
        def batch_process():
            batch = []
            import time
            last_process_time = time.time()
            
            while True:
                try:
                    # 从队列获取信号
                    signal = self.signal_queue.get(timeout=self.batch_timeout)
                    batch.append(signal)
                    
                    # 达到批量大小或超时，处理批量信号
                    current_time = time.time()
                    if len(batch) >= self.batch_size or \
                       (current_time - last_process_time) >= self.batch_timeout:
                        self.process_batch(batch)
                        batch = []
                        last_process_time = current_time
                        
                except Exception as e:
                    if batch:
                        self.process_batch(batch)
                        batch = []
                    time.sleep(0.1)
        
        thread = Thread(target=batch_process, daemon=True)
        thread.start()
    
    def process_batch(self, signals: list):
        """批量处理信号"""
        for signal in signals:
            try:
                self.convert_and_send_order(signal)
            except Exception as e:
                self.write_log(f"批量处理信号失败: {e}")
    
    def convert_and_send_order(self, signal: Dict[str, Any]):
        """转换信号为订单并发送"""
        try:
            # 解析信号
            symbol = signal.get("symbol")
            exchange_str = signal.get("exchange")
            direction_str = signal.get("direction", "LONG")
            offset_str = signal.get("offset", "OPEN")
            price = float(signal.get("price", 0))
            volume = float(signal.get("volume", 1))
            order_type_str = signal.get("order_type", "LIMIT")
            gateway_name = signal.get("gateway_name", "CTP")
            strategy_id = signal.get("strategy_id", "")
            
            # 转换交易所
            exchange_map = {
                "SHFE": Exchange.SHFE,
                "CFFEX": Exchange.CFFEX,
                "CZCE": Exchange.CZCE,
                "DCE": Exchange.DCE,
                "INE": Exchange.INE,
            }
            exchange = exchange_map.get(exchange_str)
            if not exchange:
                raise ValueError(f"不支持的交易所: {exchange_str}")
            
            # 转换方向
            direction = Dir.LONG if direction_str.upper() == "LONG" else Dir.SHORT
            
            # 转换开平
            offset_map = {
                "OPEN": Off.OPEN,
                "CLOSE": Off.CLOSE,
                "CLOSETODAY": Off.CLOSETODAY,
                "CLOSEYESTERDAY": Off.CLOSEYESTERDAY,
            }
            offset = offset_map.get(offset_str.upper(), Off.OPEN)
            
            # 转换订单类型
            order_type = OT.LIMIT if order_type_str.upper() == "LIMIT" else OT.MARKET
            
            # 创建订单请求
            req = OrderRequest(
                symbol=symbol,
                exchange=exchange,
                direction=direction,
                offset=offset,
                price=price,
                volume=volume,
                type=order_type,
                reference=strategy_id
            )
            
            # 发送订单
            vt_orderid = self.main_engine.send_order(req, gateway_name)
            
            with self.lock:
                self.order_count += 1
            
            self.write_log(
                f"信号处理成功: {strategy_id} -> {vt_orderid} | "
                f"{symbol}.{exchange_str} {direction_str} {offset_str} "
                f"{volume}@{price}"
            )
            
        except Exception as e:
            with self.lock:
                self.error_count += 1
            self.write_log(f"转换信号失败: {e} | 信号: {signal}")
    
    def register_event(self):
        """注册事件"""
        pass
    
    def write_log(self, msg: str):
        """写日志"""
        from vnpy.trader.object import LogData
        log = LogData(msg=msg, gateway_name=APP_NAME)
        event = Event(EVENT_SIGNAL_LOG, log)
        self.event_engine.put(event)
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self.lock:
            return {
                "signal_count": self.signal_count,
                "order_count": self.order_count,
                "error_count": self.error_count,
                "success_rate": (
                    self.order_count / self.signal_count * 100
                    if self.signal_count > 0 else 0
                )
            }
    
    def close(self):
        """关闭引擎"""
        self.stop()
```

#### 5. App模块初始化

```python
# vnpy/app/signal_receiver/__init__.py
from pathlib import Path
from vnpy.trader.app import BaseApp
from .engine import SignalReceiverEngine

APP_NAME = "SignalReceiver"


class SignalReceiverApp(BaseApp):
    """信号接收应用"""
    
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "信号接收"
    engine_class = SignalReceiverEngine
    widget_name = "SignalReceiverWidget"
    icon_name = "signal.ico"
```

#### 6. 在run.py中使用

```python
# run.py
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp
from vnpy.gateway.ctp import CtpGateway
from vnpy.app.signal_receiver import SignalReceiverApp

def main():
    qapp = create_qapp()
    
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    
    # 添加网关
    main_engine.add_gateway(CtpGateway)
    
    # 添加信号接收应用
    signal_engine = main_engine.add_app(SignalReceiverApp)
    
    # 启动信号接收
    signal_engine.start()
    
    # UI（可选）
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()
    
    qapp.exec()

if __name__ == "__main__":
    main()
```

---

## 📡 VN.Py支持的信号接收方式

### 1. RPC服务（ZMQ）✅ 内置支持

**实现方式**：使用ZMQ实现RPC通信

**特点**：
- ✅ VN.Py内置支持（`vnpy/app/rpc_service`）
- ✅ 支持多进程通信
- ✅ 支持局域网通信
- ✅ 高性能（ZMQ）
- ✅ 支持加密认证

**适用场景**：
- 同一机器多进程通信
- 局域网内多机器通信
- 需要高性能的场景

**使用方式**：
```python
from vnpy.app.rpc_service import RpcServiceApp

main_engine.add_app(RpcServiceApp)
# 启动RPC服务后，客户端可以调用main_engine的方法
```

**客户端调用示例**：
```python
from vnpy.rpc import RpcClient

client = RpcClient()
client.start("tcp://127.0.0.1:2014", "tcp://127.0.0.1:4102")

# 发送订单
req = OrderRequest(...)
vt_orderid = client.send_order(req, "CTP")
```

---

### 2. REST API ⚠️ 需要自行实现

**实现方式**：基于Flask/FastAPI创建HTTP服务

**特点**：
- ⚠️ VN.Py没有内置REST服务（只有REST客户端）
- ✅ 跨语言支持（任何语言都可以调用）
- ✅ 易于调试（使用浏览器/Postman）
- ✅ 支持负载均衡
- ⚠️ 性能相对较低（HTTP开销）

**适用场景**：
- 需要跨语言调用
- 需要Web界面集成
- 需要RESTful API的场景

**实现示例**：
```python
# 创建REST API服务
from flask import Flask, request, jsonify
from vnpy.trader.engine import MainEngine

app = Flask(__name__)
main_engine = None  # 需要初始化

@app.route('/api/order', methods=['POST'])
def send_order():
    data = request.json
    req = OrderRequest(**data)
    vt_orderid = main_engine.send_order(req, data['gateway_name'])
    return jsonify({"vt_orderid": vt_orderid})
```

---

### 3. WebSocket ⚠️ 需要自行实现

**实现方式**：使用VN.Py的WebSocket客户端或创建WebSocket服务

**特点**：
- ⚠️ VN.Py只有WebSocket客户端（用于连接交易所）
- ✅ 实时双向通信
- ✅ 低延迟
- ⚠️ 需要自行实现服务端

**适用场景**：
- 需要实时推送的场景
- Web前端集成
- 需要双向通信的场景

**实现示例**：
```python
# 使用websocket库创建服务
import websocket
import json
from vnpy.trader.engine import MainEngine

def on_message(ws, message):
    signal = json.loads(message)
    req = convert_signal_to_order(signal)
    main_engine.send_order(req, signal['gateway_name'])

ws = websocket.WebSocketApp("ws://localhost:8765", on_message=on_message)
ws.run_forever()
```

---

### 4. 消息队列（RabbitMQ/Redis/Kafka）✅ 推荐

**实现方式**：订阅消息队列，消费信号消息

**特点**：
- ✅ 解耦：生产者和消费者分离
- ✅ 可靠性：消息持久化
- ✅ 高并发：支持大量消息
- ✅ 可扩展：支持集群
- ✅ 支持消息确认和重试

**适用场景**：
- **高频信号场景（您的需求）**
- 需要消息持久化
- 需要消息确认机制
- 需要支持高并发

**支持的消息队列**：
1. **RabbitMQ**（您选择的）✅
   - 功能丰富
   - 支持多种消息模式
   - 管理界面友好

2. **Redis Pub/Sub**
   - 简单轻量
   - 性能高
   - 但不支持消息持久化

3. **Kafka**
   - 超高吞吐量
   - 适合大数据场景
   - 配置复杂

---

### 5. 同一机器通信（文件/共享内存/管道）

**实现方式**：使用文件、共享内存、命名管道等

**特点**：
- ✅ 简单直接
- ✅ 无需网络
- ⚠️ 仅限同一机器
- ⚠️ 性能一般

**适用场景**：
- 同一机器进程通信
- 简单场景
- 不需要高并发

**实现示例**：
```python
# 文件监听方式
import watchdog
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class SignalFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        # 读取文件，解析信号
        signal = read_signal_file(event.src_path)
        process_signal(signal)
```

---

### 6. 数据库轮询 ⚠️ 不推荐

**实现方式**：DolphinDB写入数据库，VN.Py轮询读取

**特点**：
- ⚠️ 性能差（轮询延迟）
- ⚠️ 资源浪费
- ⚠️ 不适合高频场景

**适用场景**：
- 低频信号
- 历史数据回放
- 不推荐用于实时交易

---

## 📊 方案对比与选择

| 方式 | 性能 | 可靠性 | 复杂度 | 跨语言 | 适用场景 |
|------|------|--------|--------|--------|----------|
| **RabbitMQ** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ✅ | **高频信号（推荐）** |
| RPC (ZMQ) | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ | ❌ | 多进程通信 |
| REST API | ⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ✅ | Web集成 |
| WebSocket | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ✅ | 实时推送 |
| Redis Pub/Sub | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ✅ | 简单场景 |
| Kafka | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ✅ | 超高频场景 |
| 文件/共享内存 | ⭐⭐⭐ | ⭐⭐ | ⭐ | ❌ | 同一机器 |

### 针对您的需求（1000个策略高频信号）

**推荐方案：RabbitMQ** ✅

**理由**：
1. ✅ **高并发支持**：RabbitMQ可以轻松处理大量消息
2. ✅ **消息持久化**：确保信号不丢失
3. ✅ **消息确认机制**：保证信号被正确处理
4. ✅ **解耦设计**：DolphinDB和VN.Py完全解耦
5. ✅ **易于扩展**：支持集群和负载均衡
6. ✅ **管理界面**：RabbitMQ Management提供监控

**性能优化建议**：
1. **批量处理**：收集一批信号后批量发送订单
2. **多队列分片**：按策略ID分片到不同队列
3. **预取限制**：设置合理的prefetch_count
4. **持久化配置**：消息和队列都设置持久化
5. **连接池**：复用RabbitMQ连接

---

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install pika  # RabbitMQ客户端
```

### 2. 启动RabbitMQ

```bash
# Docker方式（推荐）
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management

# 访问管理界面
# http://localhost:15672
# 默认用户名/密码: guest/guest
```

### 3. DolphinDB发送信号示例

```python
# DolphinDB端（Python脚本）
import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# 声明Exchange
channel.exchange_declare(exchange='signals', exchange_type='topic')

# 发送信号
signal = {
    "strategy_id": "strategy_001",
    "symbol": "rb2310",
    "exchange": "SHFE",
    "direction": "LONG",
    "offset": "OPEN",
    "price": 3500.0,
    "volume": 1,
    "order_type": "LIMIT",
    "gateway_name": "CTP",
    "timestamp": "2024-01-01 10:00:00"
}

channel.basic_publish(
    exchange='signals',
    routing_key='signal.order',
    body=json.dumps(signal),
    properties=pika.BasicProperties(delivery_mode=2)  # 持久化
)

connection.close()
```

### 4. VN.Py接收信号

按照上面的代码实现`SignalReceiverEngine`，然后在`run.py`中启动即可。

---

## 📝 总结

1. **RabbitMQ方案**：最适合您的高频信号场景 ✅
2. **RPC方案**：适合多进程通信，但需要VN.Py作为服务端
3. **REST/WebSocket**：需要自行实现服务端
4. **消息队列**：解耦、可靠、高性能，是高频场景的最佳选择

**建议**：使用RabbitMQ方案，按照上面的代码实现即可。

