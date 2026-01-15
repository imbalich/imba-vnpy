"""
Tick数据采集引擎
基于vnpy的BaseEngine架构，最小改动实现数据采集
"""

from datetime import datetime
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.event import EventEngine, Event
from vnpy.trader.object import TickData
from vnpy.trader.constant import Exchange
from vnpy.trader.event import EVENT_TICK


class TickCollectorEngine(BaseEngine):
    """Tick数据采集引擎"""
    
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super().__init__(main_engine, event_engine, "TickCollector")
        
        # 数据统计
        self.tick_count = 0
        self.sample_ticks = []
        
        # 注册事件监听
        self.register_event()
        
        print("✓ TickCollectorEngine 初始化完成")
    
    def register_event(self):
        """注册事件监听 - 仿照RecorderEngine"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
    
    def process_tick_event(self, event: Event):
        """处理tick事件 - 仿照RecorderEngine.process_tick_event"""
        tick: TickData = event.data
        
        # 只处理IM2603的数据
        if tick.symbol == "IM2603" and tick.exchange == Exchange.CFFEX:
            # 记录接收时间戳（关键时间点1）
            receive_timestamp = datetime.now()
            
            # 调用写入方法，传入接收时间戳
            self.write_to_stream_table(tick, receive_timestamp)
            
    def write_to_stream_table(self, tick: TickData, receive_timestamp: datetime):
        """写入流表"""
        # 记录开始写入时间戳（关键时间点2）
        write_start_timestamp = datetime.now()
        
        # 转换数据并写入
        data = self.convert_tick_to_dict(tick, receive_timestamp, write_start_timestamp)
        
        # 写入DolphinDB流表
        table = self.dolphindb_session.table(data=[data], tableAliasName="t")
        self.dolphindb_session.run("tick_stream.append!(t)", t=table)
        
        # 记录写入完成时间戳（关键时间点3）
        write_end_timestamp = datetime.now()
        
        # 更新write_end_timestamp（如果需要精确记录）
        # 注意：如果DolphinDB端使用now()，这一步可以省略

    
    def collect_tick(self, tick: TickData):
        """采集tick数据"""
        self.tick_count += 1
        
        # 打印基本信息
        print(f"[{tick.datetime}] {tick.symbol} 最新价: {tick.last_price:.2f}, 成交量: {tick.volume}")
        
        # # 保存前10条样例数据
        # if len(self.sample_ticks) < 10:
        #     self.sample_ticks.append(tick)
        #     # 打印完整数据结构（前10条）
        #     self.print_tick_structure(tick)
        
        # 每100条打印统计
        if self.tick_count % 100 == 0:
            print(f"已采集 {self.tick_count} 条tick数据")
    
    def print_tick_structure(self, tick: TickData):
        """打印tick对象的完整结构"""
        print("\n" + "="*60)
        print(f"Tick对象结构 (第{len(self.sample_ticks)}条):")
        print("="*60)
        
        # 打印所有属性
        for attr in dir(tick):
            if not attr.startswith('_'):
                try:
                    value = getattr(tick, attr)
                    if not callable(value):
                        print(f"  {attr}: {value} (类型: {type(value).__name__})")
                except:
                    pass
        
        print("="*60 + "\n")