"""
信号消息消费者
"""
import threading
from typing import Callable, Optional
from .connection import RabbitMQConnection
from ..signal.signal import TradingSignal
import logging
import pika


class SignalConsumer:
    """信号消费者 - 用于 vnpy"""
    
    QUEUE_NAME = "trading_signals"
    EXCHANGE_NAME = "trading"
    ROUTING_KEY = "signal"
    
    def __init__(
        self, 
        connection: RabbitMQConnection,
        callback: Callable[[TradingSignal], None]
    ):
        self.connection = connection
        self.callback = callback
        self.logger = logging.getLogger(__name__)
        self._running = False
        self._thread: Optional[threading.Thread] = None
    
    def _setup_queue(self, channel):
        """确保队列和交换机存在（如果不存在则创建）"""
        # 声明交换机
        channel.exchange_declare(
            exchange=self.EXCHANGE_NAME,
            exchange_type='direct',
            durable=True
        )
        
        # 声明队列
        channel.queue_declare(
            queue=self.QUEUE_NAME,
            durable=True
        )
        
        # 绑定队列到交换机
        channel.queue_bind(
            exchange=self.EXCHANGE_NAME,
            queue=self.QUEUE_NAME,
            routing_key=self.ROUTING_KEY
        )
        
        self.logger.info(f"队列设置完成: {self.QUEUE_NAME}")
    
    def _on_message(self, channel, method, properties, body):
        """消息回调"""
        try:
            signal = TradingSignal.from_json(body.decode('utf-8'))
            self.callback(signal)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            self.logger.error(f"处理信号失败: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    def start(self):
        """启动消费者（在后台线程中运行）"""
        self._running = True
        self._thread = threading.Thread(target=self._consume, daemon=True)
        self._thread.start()
        self.logger.info("信号消费者已启动")
    
    def _consume(self):
        """消费消息"""
        channel = self.connection.channel
        
        # 确保队列存在（如果不存在则创建）
        self._setup_queue(channel)
        
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.QUEUE_NAME,
            on_message_callback=self._on_message
        )
        
        while self._running:
            try:
                self.connection._connection.process_data_events(time_limit=1)
            except Exception as e:
                if self._running:
                    self.logger.error(f"消费者错误: {e}")
    
    def stop(self):
        """停止消费者"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        self.connection.close()
        self.logger.info("信号消费者已停止")