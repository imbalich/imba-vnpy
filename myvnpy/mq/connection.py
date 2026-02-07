"""
RabbitMQ 连接管理
"""
import pika
from typing import Optional
import logging


class RabbitMQConnection:
    """RabbitMQ 连接管理器"""
    
    def __init__(
        self, 
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        virtual_host: str = "/"
    ):
        self.host = host
        self.port = port
        self.credentials = pika.PlainCredentials(username, password)
        self.virtual_host = virtual_host
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.channel.Channel] = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> pika.channel.Channel:
        """建立连接"""
        if self._connection is None or self._connection.is_closed:
            params = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=self.credentials,
                virtual_host=self.virtual_host,
                heartbeat=60
            )
            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()
            self.logger.info(f"已连接到 RabbitMQ {self.host}:{self.port}")
        return self._channel
    
    def close(self):
        """关闭连接"""
        if self._connection and not self._connection.is_closed:
            self._connection.close()
            self.logger.info("RabbitMQ 连接已关闭")
    
    @property
    def channel(self) -> pika.channel.Channel:
        return self.connect()