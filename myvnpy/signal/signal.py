"""
交易信号数据结构
"""
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from typing import Optional
import json


class SignalType(Enum):
    """信号类型"""
    BUY = "buy"           # 买入
    SELL = "sell"         # 卖出
    CLOSE_LONG = "close_long"   # 平多
    CLOSE_SHORT = "close_short" # 平空
    HOLD = "hold"         # 持仓观望


class SignalStrength(Enum):
    """信号强度"""
    STRONG = "strong"     # 强信号
    NORMAL = "normal"     # 普通信号
    WEAK = "weak"         # 弱信号


@dataclass
class TradingSignal:
    """交易信号"""
    signal_id: str              # 信号唯一ID
    symbol: str                 # 合约代码
    signal_type: SignalType     # 信号类型
    signal_strength: SignalStrength  # 信号强度
    factor_value: float         # 因子值
    price: Optional[float] = None   # 建议价格（可选，默认None）
    volume: int = 1             # 建议数量
    timestamp: datetime = None  # 信号生成时间
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def to_json(self) -> str:
        """序列化为JSON"""
        data = asdict(self)
        data['signal_type'] = self.signal_type.value
        data['signal_strength'] = self.signal_strength.value
        data['timestamp'] = self.timestamp.isoformat()
        return json.dumps(data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'TradingSignal':
        """从JSON反序列化"""
        data = json.loads(json_str)
        data['signal_type'] = SignalType(data['signal_type'])
        data['signal_strength'] = SignalStrength(data['signal_strength'])
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)