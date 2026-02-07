"""
信号执行引擎 - 接收信号并通过CTP下单
"""
from vnpy.event import EventEngine, Event
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.object import OrderRequest, SubscribeRequest
from vnpy.trader.constant import Direction, Offset, OrderType, Exchange

from ...mq.connection import RabbitMQConnection
from ...mq.consumer import SignalConsumer
from ...signal.signal import TradingSignal, SignalType

import logging
from typing import Dict


class SignalExecutorEngine(BaseEngine):
    """信号执行引擎"""
    
    engine_name = "SignalExecutor"
    
    def __init__(
        self,
        main_engine: MainEngine,
        event_engine: EventEngine,
        gateway_name: str = "CTP",
        mq_host: str = "localhost",
        mq_port: int = 5672,
        mq_user: str = "guest",
        mq_password: str = "guest"
    ):
        super().__init__(main_engine, event_engine, self.engine_name)
        
        self.gateway_name = gateway_name
        self.logger = logging.getLogger(__name__)
        
        # 持仓管理
        self.positions: Dict[str, int] = {}  # symbol -> position
        
        # 初始化MQ消费者
        self.mq_connection = RabbitMQConnection(
            host=mq_host,
            port=mq_port,
            username=mq_user,
            password=mq_password
        )
        self.consumer = SignalConsumer(
            connection=self.mq_connection,
            callback=self.on_signal
        )
    
    def start(self):
        """启动引擎"""
        self.consumer.start()
        self.write_log("信号执行引擎已启动")
    
    def stop(self):
        """停止引擎"""
        self.consumer.stop()
        self.write_log("信号执行引擎已停止")
    
    def on_signal(self, signal: TradingSignal):
        """处理信号"""
        self.write_log(f"收到信号: {signal.symbol} {signal.signal_type.value} "
                       f"因子值={signal.factor_value:.4f}")
        
        # 根据信号类型执行操作
        if signal.signal_type == SignalType.BUY:
            self.execute_buy(signal)
        elif signal.signal_type == SignalType.SELL:
            self.execute_sell(signal)
        elif signal.signal_type == SignalType.CLOSE_LONG:
            self.execute_close_long(signal)
        elif signal.signal_type == SignalType.CLOSE_SHORT:
            self.execute_close_short(signal)
    
    def _get_price(self, signal: TradingSignal, use_ask: bool) -> float:
        """
        获取下单价格
        
        Args:
            signal: 交易信号
            use_ask: True=使用卖一价（买入时），False=使用买一价（卖出时）
        
        Returns:
            下单价格，如果无法获取返回 None
        """
        # 优先使用信号中的价格
        if signal.price:
            return signal.price
        
        # 否则获取当前行情的对手价
        symbol, exchange = self._parse_symbol(signal.symbol)
        vt_symbol = f"{symbol}.{exchange.value}"
        tick = self.main_engine.get_tick(vt_symbol)
        
        if tick:
            if use_ask:
                price = tick.ask_price_1  # 买入用卖一价
            else:
                price = tick.bid_price_1  # 卖出用买一价
            self.write_log(f"使用对手价: {price}")
            return price
        else:
            self.write_log(f"警告：无法获取 {vt_symbol} 的行情")
            return None
    
    def execute_buy(self, signal: TradingSignal):
        """执行买入（开多）"""
        symbol, exchange = self._parse_symbol(signal.symbol)
        
        # 获取价格（买入用卖一价）
        price = self._get_price(signal, use_ask=True)
        if price is None:
            self.write_log(f"买入失败：无法获取 {symbol} 的价格")
            return
        
        req = OrderRequest(
            symbol=symbol,
            exchange=exchange,
            direction=Direction.LONG,
            type=OrderType.LIMIT,
            offset=Offset.OPEN,
            price=price,
            volume=signal.volume
        )
        
        order_id = self.main_engine.send_order(req, self.gateway_name)
        self.write_log(f"发送买入订单: {order_id}, 价格: {price}")
    
    def execute_sell(self, signal: TradingSignal):
        """执行卖出（开空）"""
        symbol, exchange = self._parse_symbol(signal.symbol)
        
        # 获取价格（卖出用买一价）
        price = self._get_price(signal, use_ask=False)
        if price is None:
            self.write_log(f"卖出失败：无法获取 {symbol} 的价格")
            return
        
        req = OrderRequest(
            symbol=symbol,
            exchange=exchange,
            direction=Direction.SHORT,
            type=OrderType.LIMIT,
            offset=Offset.OPEN,
            price=price,
            volume=signal.volume
        )
        
        order_id = self.main_engine.send_order(req, self.gateway_name)
        self.write_log(f"发送卖出订单: {order_id}, 价格: {price}")
    
    def execute_close_long(self, signal: TradingSignal):
        """平多"""
        symbol, exchange = self._parse_symbol(signal.symbol)
        
        # 获取价格（平多用买一价）
        price = self._get_price(signal, use_ask=False)
        if price is None:
            self.write_log(f"平多失败：无法获取 {symbol} 的价格")
            return
        
        req = OrderRequest(
            symbol=symbol,
            exchange=exchange,
            direction=Direction.SHORT,
            type=OrderType.LIMIT,
            offset=Offset.CLOSE,
            price=price,
            volume=signal.volume
        )
        
        order_id = self.main_engine.send_order(req, self.gateway_name)
        self.write_log(f"发送平多订单: {order_id}, 价格: {price}")
    
    def execute_close_short(self, signal: TradingSignal):
        """平空"""
        symbol, exchange = self._parse_symbol(signal.symbol)
        
        # 获取价格（平空用卖一价）
        price = self._get_price(signal, use_ask=True)
        if price is None:
            self.write_log(f"平空失败：无法获取 {symbol} 的价格")
            return
        
        req = OrderRequest(
            symbol=symbol,
            exchange=exchange,
            direction=Direction.LONG,
            type=OrderType.LIMIT,
            offset=Offset.CLOSE,
            price=price,
            volume=signal.volume
        )
        
        order_id = self.main_engine.send_order(req, self.gateway_name)
        self.write_log(f"发送平空订单: {order_id}, 价格: {price}")
    
    def _parse_symbol(self, symbol: str):
        """解析合约代码（假设格式为 IM2603 或 IM2603.CFFEX）"""
        if '.' in symbol:
            parts = symbol.split('.')
            return parts[0], Exchange(parts[1])
        else:
            # 默认中金所
            return symbol, Exchange.CFFEX
    
    def write_log(self, msg: str):
        """写日志"""
        self.logger.info(msg)
        print(f"[SignalExecutor] {msg}")