"""
Tick数据转换器
职责：将TickData对象转换为符合DolphinDB流表结构的字典
"""

from datetime import datetime
from typing import Dict
from vnpy.trader.object import TickData
from vnpy.trader.constant import Exchange


def convert_tick_to_dict(
    tick: TickData,
    receive_timestamp_ns: int
) -> Dict:
    """
    将TickData转换为符合DolphinDB流表结构的字典
    
    Args:
        tick: TickData对象
        receive_timestamp_ns: 接收时间戳（纳秒级，int类型）
        
    Returns:
        dict: 符合DolphinDB流表结构的字典
        
    注意：
    - write_start_timestamp 字段设为 None，由 StreamTableWriter 负责填充
    - write_end_timestamp 字段设为 None，由 DolphinDB 订阅函数负责填充
        
    字段说明（与creat_stream_table.dos中的结构一致）：
    - symbol: STRING
    - exchange: STRING
    - trade_date: DATE (从datetime提取)
    - datetime: TIMESTAMP
    - name: STRING
    - last_price: DOUBLE
    - open_price: DOUBLE
    - high_price: DOUBLE
    - low_price: DOUBLE
    - pre_close: DOUBLE
    - limit_up: DOUBLE
    - limit_down: DOUBLE
    - volume: DOUBLE
    - last_volume: DOUBLE
    - open_interest: DOUBLE
    - bid_price_1: DOUBLE
    - bid_volume_1: DOUBLE
    - ask_price_1: DOUBLE
    - ask_volume_1: DOUBLE
    - gateway_name: STRING
    - receive_timestamp: NANOTIMESTAMP (纳秒级时间戳)
    - write_start_timestamp: NANOTIMESTAMP (纳秒级时间戳)
    - write_end_timestamp: NANOTIMESTAMP (纳秒级时间戳，可选，由DolphinDB端填充)
    """
    # 处理datetime和trade_date
    tick_datetime = tick.datetime
    if tick_datetime is None:
        # 如果没有datetime，使用当前时间
        tick_datetime = datetime.now()

    # 移除时区信息，保持本地时间（不转换为UTC）
    # 这样确保数据库中存储的是本地时间
    if hasattr(tick_datetime, 'tzinfo') and tick_datetime.tzinfo is not None:
        # 直接移除时区信息，保持时间值不变
        tick_datetime = tick_datetime.replace(tzinfo=None)

    # 提取交易日期（DATE类型）
    trade_date = tick_datetime.date()

    # 转换exchange为字符串
    exchange_str = tick.exchange.value if isinstance(tick.exchange, Exchange) else str(tick.exchange)

    # 构建字典（按照DolphinDB流表的字段顺序）
    data = {
        # 基本信息
        "symbol": tick.symbol,
        "exchange": exchange_str,
        "trade_date": trade_date,  # DATE类型
        "datetime": tick_datetime.strftime('%Y-%m-%d %H:%M:%S.%f') if tick_datetime else None,  # TIMESTAMP类型，转为字符串避免pandas时区问题
        "name": tick.name or "",
        
        # 价格信息
        "last_price": float(tick.last_price) if tick.last_price else 0.0,
        "open_price": float(tick.open_price) if tick.open_price else 0.0,
        "high_price": float(tick.high_price) if tick.high_price else 0.0,
        "low_price": float(tick.low_price) if tick.low_price else 0.0,
        "pre_close": float(tick.pre_close) if tick.pre_close else 0.0,
        "limit_up": float(tick.limit_up) if tick.limit_up else 0.0,
        "limit_down": float(tick.limit_down) if tick.limit_down else 0.0,
        
        # 成交量信息
        "volume": float(tick.volume) if tick.volume else 0.0,
        "last_volume": float(tick.last_volume) if tick.last_volume else 0.0,
        "open_interest": float(tick.open_interest) if tick.open_interest else 0.0,
        
        # 买卖盘信息（仅1档）
        "bid_price_1": float(tick.bid_price_1) if tick.bid_price_1 else 0.0,
        "bid_volume_1": float(tick.bid_volume_1) if tick.bid_volume_1 else 0.0,
        "ask_price_1": float(tick.ask_price_1) if tick.ask_price_1 else 0.0,
        "ask_volume_1": float(tick.ask_volume_1) if tick.ask_volume_1 else 0.0,
        
        # 网关信息
        "gateway_name": tick.gateway_name or "",
        
        # 时间戳字段（纳秒级）
        # 注意：DolphinDB的NANOTIMESTAMP类型需要纳秒级时间戳
        # 这里传递纳秒级时间戳（int类型），写入时由DolphinDB Python API转换为NANOTIMESTAMP
        "receive_timestamp": receive_timestamp_ns,  # NANOTIMESTAMP - 由TickCollectorEngine记录
        "write_start_timestamp": None,  # NANOTIMESTAMP - 由StreamTableWriter填充
        "write_end_timestamp": None,  # NANOTIMESTAMP - 由DolphinDB订阅函数填充
    }
    
    return data


class TickDataConverter:
    """
    Tick数据转换器类
    
    职责：
    1. 将TickData转换为字典格式
    2. 添加时间戳字段（纳秒级）
    3. 处理字段映射和类型转换
    4. 确保与DolphinDB流表结构一致
    """
    
    def __init__(self):
        """初始化转换器"""
        pass
    
    def convert(
        self,
        tick: TickData,
        receive_timestamp_ns: int
    ) -> Dict:
        """
        转换TickData为字典
        
        Args:
            tick: TickData对象
            receive_timestamp_ns: 接收时间戳（纳秒级）
            
        Returns:
            dict: 符合DolphinDB流表结构的字典
            
        注意：
        - write_start_timestamp 字段设为 None，由 StreamTableWriter 负责填充
        - write_end_timestamp 字段设为 None，由 DolphinDB 订阅函数负责填充
        """
        return convert_tick_to_dict(tick, receive_timestamp_ns)
    
    def __call__(
        self,
        tick: TickData,
        receive_timestamp_ns: int
    ) -> Dict:
        """
        使转换器可调用（支持函数式调用）
        
        Args:
            tick: TickData对象
            receive_timestamp_ns: 接收时间戳（纳秒级）
            
        Returns:
            dict: 符合DolphinDB流表结构的字典
        """
        return self.convert(tick, receive_timestamp_ns)
