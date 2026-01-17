"""
DolphinDB连接会话管理
职责：管理DolphinDB连接，提供单例模式，支持连接重试和健康检查
"""

import time
from typing import Optional
import dolphindb as ddb


class DolphinDBSession:
    """
    DolphinDB连接会话管理器（单例模式）
    
    职责：
    1. 管理DolphinDB连接
    2. 提供连接健康检查
    3. 支持自动重连
    4. 线程安全（单例模式）
    """
    
    _instance: Optional['DolphinDBSession'] = None
    _lock = None
    
    def __new__(cls, host: str = "localhost", port: int = 8848, 
                user: str = "admin", password: str = "123456"):
        """
        单例模式实现
        
        Args:
            host: DolphinDB服务器地址
            port: DolphinDB服务器端口
            user: 用户名
            password: 密码
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, host: str = "localhost", port: int = 8848,
                 user: str = "admin", password: str = "123456"):
        """
        初始化DolphinDB连接（单例模式，多次调用不会重复初始化）
        
        Args:
            host: DolphinDB服务器地址
            port: DolphinDB服务器端口
            user: 用户名
            password: 密码
        """
        if self._initialized:
            return
        
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.session: Optional[ddb.session] = None
        self._initialized = True
        
        # 连接DolphinDB
        self._connect()
    
    def _connect(self):
        """
        连接DolphinDB服务器
        
        Raises:
            ConnectionError: 连接失败时抛出异常
        """
        try:
            # 如果已有连接，先关闭
            if self.session is not None:
                try:
                    self.session.close()
                except:
                    pass
            
            # 创建新连接
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
    
    def is_connected(self) -> bool:
        """
        检查连接是否有效
        
        Returns:
            bool: True表示连接有效，False表示连接无效
        """
        if self.session is None:
            return False
        
        try:
            # 执行简单查询验证连接
            self.session.run("1+1")
            return True
        except Exception:
            return False
    
    def reconnect(self, max_retries: int = 3, retry_interval: float = 1.0):
        """
        重新连接DolphinDB
        
        Args:
            max_retries: 最大重试次数
            retry_interval: 重试间隔（秒）
            
        Raises:
            ConnectionError: 重试失败后抛出异常
        """
        for attempt in range(max_retries):
            try:
                print(f"尝试重新连接DolphinDB (第{attempt + 1}/{max_retries}次)...")
                self._connect()
                print("✓ DolphinDB重连成功")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"重连失败，{retry_interval}秒后重试...")
                    time.sleep(retry_interval)
                else:
                    raise ConnectionError(f"DolphinDB重连失败（已重试{max_retries}次）: {e}")
    
    def ensure_connected(self):
        """
        确保连接有效，如果无效则尝试重连
        
        Raises:
            ConnectionError: 连接无效且重连失败时抛出异常
        """
        if not self.is_connected():
            self.reconnect()
    
    def run(self, script: str, *args, **kwargs):
        """
        执行DolphinDB脚本
        
        Args:
            script: DolphinDB脚本
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            脚本执行结果
            
        Raises:
            ConnectionError: 连接无效时抛出异常
        """
        self.ensure_connected()
        return self.session.run(script, *args, **kwargs)
    
    def close(self):
        """
        关闭连接
        """
        if self.session is not None:
            try:
                self.session.close()
                print("✓ DolphinDB连接已关闭")
            except Exception as e:
                print(f"关闭连接时出错: {e}")
            finally:
                self.session = None
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        # 注意：单例模式下，不自动关闭连接
        # 如果需要关闭，请显式调用 close()
        pass
