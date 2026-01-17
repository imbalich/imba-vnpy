"""
Tick数据采集引擎
基于vnpy的BaseEngine架构，实现数据采集和过滤
职责：监听tick事件、过滤数据、记录时间戳、调用转换层
"""

import time
from datetime import datetime, timezone
from typing import Optional, Dict, List, Set, Callable
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.event import EventEngine, Event
from vnpy.trader.object import TickData
from vnpy.trader.constant import Exchange
from vnpy.trader.event import EVENT_TICK


def get_nanosecond_timestamp() -> int:
    """
    获取纳秒级时间戳（Unix时间戳，纳秒精度）
    
    Returns:
        int: 纳秒级时间戳（自1970-01-01 00:00:00 UTC以来的纳秒数）
    """
    return time.time_ns()


def nanosecond_timestamp_to_datetime(ns_timestamp: int) -> datetime:
    """
    将纳秒级时间戳转换为datetime对象（保留纳秒信息）
    
    Args:
        ns_timestamp: 纳秒级时间戳
        
    Returns:
        datetime: 带时区信息的datetime对象（UTC时区）
    """
    # 转换为秒和纳秒
    seconds = ns_timestamp // 1_000_000_000
    nanoseconds = ns_timestamp % 1_000_000_000
    
    # 创建datetime对象（UTC时区）
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    
    # 添加纳秒信息（通过microseconds字段，但实际精度更高）
    # 注意：Python的datetime对象本身只支持微秒级精度
    # 但我们可以保留纳秒信息，在需要时传递给DolphinDB
    return dt.replace(microsecond=nanoseconds // 1000)


class FilterConfig:
    """过滤配置类"""
    
    def __init__(
        self,
        symbols: Optional[List[str]] = None,
        exchanges: Optional[List[Exchange]] = None,
        custom_filter: Optional[Callable[[TickData], bool]] = None
    ):
        """
        初始化过滤配置
        
        Args:
            symbols: 允许的symbol列表，None表示不过滤
            exchanges: 允许的exchange列表，None表示不过滤
            custom_filter: 自定义过滤函数，接受TickData返回bool
        """
        self.symbols: Optional[Set[str]] = set(symbols) if symbols else None
        self.exchanges: Optional[Set[Exchange]] = set(exchanges) if exchanges else None
        self.custom_filter = custom_filter
    
    def should_collect(self, tick: TickData) -> bool:
        """
        判断是否应该采集该tick数据
        
        Args:
            tick: TickData对象
            
        Returns:
            bool: True表示应该采集，False表示跳过
        """
        # 检查symbol过滤
        if self.symbols is not None and tick.symbol not in self.symbols:
            return False
        
        # 检查exchange过滤
        if self.exchanges is not None and tick.exchange not in self.exchanges:
            return False
        
        # 检查自定义过滤函数
        if self.custom_filter is not None and not self.custom_filter(tick):
            return False
        
        return True


class TickCollectorEngine(BaseEngine):
    """
    Tick数据采集引擎
    
    职责：
    1. 监听EVENT_TICK事件
    2. 根据配置过滤数据
    3. 记录接收时间戳（receive_timestamp）
    4. 调用转换层进行数据转换（如果提供了converter）
    
    不负责：
    - 数据转换（由TickDataConverter负责）
    - 数据写入（由StreamTableWriter负责）
    - DolphinDB连接管理
    """
    
    def __init__(
        self,
        main_engine: MainEngine,
        event_engine: EventEngine,
        converter: Optional[Callable[[TickData, int], dict]] = None,
        writer: Optional[object] = None,
        filter_config: Optional[FilterConfig] = None,
        enable_statistics: bool = True,
        debug: bool = True
    ):
        """
        初始化TickCollectorEngine
        
        Args:
            main_engine: VN Trader主引擎
            event_engine: 事件引擎
            converter: 数据转换器，接受(tick, receive_timestamp_ns)返回dict
                      receive_timestamp_ns是纳秒级时间戳（int类型）
            writer: 数据写入器，提供write(data)方法（可选）
            filter_config: 过滤配置，None表示采集所有tick
            enable_statistics: 是否启用统计功能
            debug: 是否启用调试模式（打印详细信息）
        """
        super().__init__(main_engine, event_engine, "TickCollector")
        
        # 依赖注入：转换器（可选）
        self.converter = converter
        
        # 依赖注入：写入器（可选）
        self.writer = writer
        
        # 过滤配置
        if filter_config is None:
            # 默认配置：只采集IM2603.CFFEX（保持向后兼容）
            self.filter_config = FilterConfig(
                symbols=["IM2603"],
                exchanges=[Exchange.CFFEX]
            )
        else:
            self.filter_config = filter_config
        
        # 统计功能（可选）
        self.enable_statistics = enable_statistics
        self.debug = debug
        self.tick_count = 0
        self.sample_ticks: List[TickData] = []
        self.filtered_count = 0  # 被过滤掉的tick数量
        
        # 注册事件监听
        self.register_event()
        
        print("✓ TickCollectorEngine 初始化完成")
        if self.converter:
            print("  - 已配置数据转换器")
        else:
            print("  - 未配置数据转换器（仅采集，不转换）")
        if self.writer:
            print("  - 已配置数据写入器")
        else:
            print("  - 未配置数据写入器（仅转换，不写入）")
        print(f"  - 过滤配置: symbols={self.filter_config.symbols}, exchanges={self.filter_config.exchanges}")
        print(f"  - 调试模式: {'启用' if debug else '禁用'}")
    
    def register_event(self):
        """注册事件监听"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
    
    def process_tick_event(self, event: Event):
        """
        处理tick事件
        
        流程：
        1. 提取TickData
        2. 应用过滤规则
        3. 记录接收时间戳
        4. 调用转换器（如果存在）
        5. 更新统计（如果启用）
        """
        tick: TickData = event.data
        
        # 调试：打印所有接收到的tick（前10条）
        if self.debug and self.tick_count + self.filtered_count < 10:
            print(f"[调试] 接收到tick: {tick.symbol}.{tick.exchange.value} | "
                  f"价格={tick.last_price:.2f} | 成交量={tick.volume}")
        
        # 应用过滤规则
        if not self.filter_config.should_collect(tick):
            self.filtered_count += 1
            # 调试：打印被过滤的tick（前10条）
            if self.debug and self.filtered_count <= 10:
                print(f"[过滤] 跳过tick: {tick.symbol}.{tick.exchange.value} "
                      f"(不匹配过滤规则)")
            return
        
        # 记录接收时间戳（关键时间点1：事件接收时刻，纳秒级精度）
        receive_timestamp_ns = get_nanosecond_timestamp()
        
        # 调试：打印通过过滤的tick详情（前5条）
        if self.debug and self.tick_count < 5:
            dt_str = tick.datetime.strftime("%Y-%m-%d %H:%M:%S.%f") if tick.datetime else "N/A"
            print(f"[采集] ✓ Tick #{self.tick_count + 1}: {tick.symbol}.{tick.exchange.value}")
            print(f"       时间: {dt_str}")
            print(f"       价格: {tick.last_price:.2f} | 成交量: {tick.volume}")
            print(f"       接收时间戳(ns): {receive_timestamp_ns}")
            print(f"       接收时间戳(可读): {nanosecond_timestamp_to_datetime(receive_timestamp_ns)}")
            
            # 打印完整的tick数据结构（前3条）
            if self.tick_count < 3:
                self._print_full_tick_data(tick)
        
        # 调用转换器（如果存在）
        if self.converter:
            try:
                # 调用转换器（只传入tick和receive_timestamp）
                # write_start_timestamp 由 StreamTableWriter 负责记录
                converted_data = self.converter(tick, receive_timestamp_ns)
                
                if self.debug and self.tick_count < 3:
                    print(f"       转换成功: {len(converted_data)} 个字段")
                
                # 调用写入器（如果存在）
                if self.writer:
                    try:
                        success = self.writer.write(converted_data)
                        if self.debug and self.tick_count < 3:
                            if success:
                                print(f"       写入成功")
                            else:
                                print(f"       写入失败")
                    except Exception as write_error:
                        print(f"✗ 数据写入失败: {write_error}")
                        import traceback
                        traceback.print_exc()
                        return
            except Exception as e:
                print(f"✗ 数据转换失败: {e}")
                import traceback
                traceback.print_exc()
                return
        
        # 更新统计（如果启用）
        if self.enable_statistics:
            self._update_statistics(tick)
    
    def _print_full_tick_data(self, tick: TickData):
        """
        打印完整的tick数据结构（调试用）
        
        Args:
            tick: TickData对象
        """
        print(f"\n{'='*70}")
        print(f"完整Tick数据结构 (第{self.tick_count + 1}条):")
        print(f"{'='*70}")
        
        # 基本信息
        print("\n【基本信息】")
        print(f"  symbol: {tick.symbol}")
        print(f"  exchange: {tick.exchange} ({tick.exchange.value})")
        print(f"  vt_symbol: {tick.vt_symbol}")
        print(f"  name: {tick.name}")
        print(f"  gateway_name: {tick.gateway_name}")
        print(f"  datetime: {tick.datetime}")
        
        # 价格信息
        print("\n【价格信息】")
        print(f"  last_price: {tick.last_price}")
        print(f"  open_price: {tick.open_price}")
        print(f"  high_price: {tick.high_price}")
        print(f"  low_price: {tick.low_price}")
        print(f"  pre_close: {tick.pre_close}")
        print(f"  limit_up: {tick.limit_up}")
        print(f"  limit_down: {tick.limit_down}")
        
        # 成交量信息
        print("\n【成交量信息】")
        print(f"  volume: {tick.volume}")
        print(f"  last_volume: {tick.last_volume}")
        print(f"  open_interest: {tick.open_interest}")
        
        # 买盘信息（5档）
        print("\n【买盘信息（5档）】")
        print(f"  bid_price_1: {tick.bid_price_1} | bid_volume_1: {tick.bid_volume_1}")
        print(f"  bid_price_2: {tick.bid_price_2} | bid_volume_2: {tick.bid_volume_2}")
        print(f"  bid_price_3: {tick.bid_price_3} | bid_volume_3: {tick.bid_volume_3}")
        print(f"  bid_price_4: {tick.bid_price_4} | bid_volume_4: {tick.bid_volume_4}")
        print(f"  bid_price_5: {tick.bid_price_5} | bid_volume_5: {tick.bid_volume_5}")
        
        # 卖盘信息（5档）
        print("\n【卖盘信息（5档）】")
        print(f"  ask_price_1: {tick.ask_price_1} | ask_volume_1: {tick.ask_volume_1}")
        print(f"  ask_price_2: {tick.ask_price_2} | ask_volume_2: {tick.ask_volume_2}")
        print(f"  ask_price_3: {tick.ask_price_3} | ask_volume_3: {tick.ask_volume_3}")
        print(f"  ask_price_4: {tick.ask_price_4} | ask_volume_4: {tick.ask_volume_4}")
        print(f"  ask_price_5: {tick.ask_price_5} | ask_volume_5: {tick.ask_volume_5}")
        
        # 打印所有属性（包括可能的扩展属性）
        print("\n【所有属性（通过dir）】")
        tick_attrs = [attr for attr in dir(tick) if not attr.startswith('_') and not callable(getattr(tick, attr, None))]
        for attr in sorted(tick_attrs):
            try:
                value = getattr(tick, attr)
                # 格式化显示
                if isinstance(value, float):
                    if value == 0.0:
                        value_str = "0.0"
                    else:
                        value_str = f"{value:.6f}"
                else:
                    value_str = str(value)
                print(f"  {attr}: {value_str} (类型: {type(value).__name__})")
            except Exception as e:
                print(f"  {attr}: <无法获取值: {e}>")
        
        print(f"{'='*70}\n")
    
    def _update_statistics(self, tick: TickData):
        """更新统计数据"""
        self.tick_count += 1
        
        # 每100条打印统计
        if self.tick_count % 100 == 0:
            print(f"[统计] 已采集 {self.tick_count} 条tick数据 | "
                  f"已过滤 {self.filtered_count} 条tick数据")
    
    def get_statistics(self) -> Dict:
        """获取统计数据"""
        return {
            "tick_count": self.tick_count,
            "filtered_count": self.filtered_count,
            "sample_count": len(self.sample_ticks)
        }
    
    def set_converter(self, converter: Callable[[TickData, int], dict]):
        """
        设置数据转换器（支持运行时设置）
        
        Args:
            converter: 转换函数，接受(tick, receive_timestamp_ns)返回dict
                      receive_timestamp_ns是纳秒级时间戳（int类型）
        """
        self.converter = converter
        print("✓ 数据转换器已更新")
    
    def set_filter_config(self, filter_config: FilterConfig):
        """
        设置过滤配置（支持运行时设置）
        
        Args:
            filter_config: 过滤配置对象
        """
        self.filter_config = filter_config
        print(f"✓ 过滤配置已更新: symbols={filter_config.symbols}, exchanges={filter_config.exchanges}")