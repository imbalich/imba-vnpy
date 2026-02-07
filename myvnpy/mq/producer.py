"""
信号消息生产者
"""
from typing import Optional

import pika
from .connection import RabbitMQConnection
from ..signal.signal import TradingSignal
import logging


class SignalProducer:
    """信号生产者 - 用于 get_factors.py"""
    
    QUEUE_NAME = "trading_signals"
    EXCHANGE_NAME = "trading"
    ROUTING_KEY = "signal"
    
    def __init__(self, connection: RabbitMQConnection):
        self.connection = connection
        self.logger = logging.getLogger(__name__)
        self._setup_queue()
    
    def _setup_queue(self):
        """设置队列和交换机"""
        channel = self.connection.channel
        
        # 声明交换机
        channel.exchange_declare(
            exchange=self.EXCHANGE_NAME,
            exchange_type='direct',
            durable=True
        )
        
        # 声明队列
        channel.queue_declare(
            queue=self.QUEUE_NAME,
            durable=True  # 持久化
        )
        
        # 绑定队列到交换机
        channel.queue_bind(
            exchange=self.EXCHANGE_NAME,
            queue=self.QUEUE_NAME,
            routing_key=self.ROUTING_KEY
        )
        
        self.logger.info(f"队列设置完成: {self.QUEUE_NAME}")
    
    def publish(self, signal: TradingSignal):
        """发布信号"""
        channel = self.connection.channel
        
        channel.basic_publish(
            exchange=self.EXCHANGE_NAME,
            routing_key=self.ROUTING_KEY,
            body=signal.to_json(),
            properties=pika.BasicProperties(
                delivery_mode=2,  # 消息持久化
                content_type='application/json'
            )
        )
        
        self.logger.debug(f"已发布信号: {signal.symbol} {signal.signal_type.value}")
    
    def close(self):
        """关闭连接"""
        self.connection.close()