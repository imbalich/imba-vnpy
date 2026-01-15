"""
DolphinDB Tick数据记录器
用于将vnpy接收到的tick数据实时写入DolphinDB数据库
"""

import sys
from datetime import datetime
from typing import Optional
from queue import Queue, Empty
from threading import Thread
import dolphindb as ddb

class DolphinDBRecorder:
    """DolphinDB数据记录器"""
    
    def __init__(self, host: str = "localhost", port: int = 8848, user: str = "admin", password: str = "123456"):
        """初始化DolphinDB连接"""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.session: Optional[ddb.session] = None
        
        # 连接DolphinDB
        self._connect()
        
    def _connect(self):
        """连接DolphinDB"""
        try:
            self.session = ddb.session()
            connected = self.session.connect(self.host, self.port, self.user, self.password)
            
            # 验证连接是否真的成功
            if not connected:
                raise ConnectionError(f"连接返回False，连接失败")
            
            # 尝试执行一个简单查询来验证连接
            try:
                self.session.run("1+1")
            except Exception as verify_error:
                raise ConnectionError(f"连接验证失败: {verify_error}")
            
            print(f"✓ DolphinDB连接成功: {self.host}:{self.port}")
        except Exception as e:
            print(f"✗ DolphinDB连接失败: {e}")
            self.session = None
            raise