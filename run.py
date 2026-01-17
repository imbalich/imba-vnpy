from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from myvnpy.gateway.ctp.ctp_gateway import FixedCtpGateway
from vnpy.app.cta_strategy import CtaStrategyApp
from vnpy.app.cta_backtester import CtaBacktesterApp

# 导入数据记录模块的所有组件
from myvnpy.app.data_recorder import (
    DolphinDBSession,
    StreamTableWriter,
    TickDataConverter,
    TickCollectorEngine,
    FilterConfig
)
from vnpy.trader.constant import Exchange


def main():
    """Start VN Trader"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    
    # 添加网关和应用
    main_engine.add_gateway(FixedCtpGateway)
    main_engine.add_app(CtaStrategyApp)
    main_engine.add_app(CtaBacktesterApp)

    # ============================================
    # 数据记录模块初始化（按依赖顺序）
    # ============================================
    
    # 1. 创建DolphinDB连接会话（单例模式）
    # 注意：DolphinDB服务器需要先启动，流表需要先创建
    try:
        db_session = DolphinDBSession(
            host="localhost",      # DolphinDB服务器地址
            port=8848,             # DolphinDB服务器端口
            user="admin",          # 用户名
            password="123456"      # 密码
        )
    except Exception as e:
        print(f"✗ DolphinDB连接失败，数据记录功能将不可用: {e}")
        print("  请确保：")
        print("  1. DolphinDB服务器已启动")
        print("  2. 已执行 creat_stream_table.dos 创建流表")
        print("  3. 连接参数正确")
        db_session = None
    
    # 2. 创建流表写入器（如果连接成功）
    writer = None
    if db_session:
        try:
            writer = StreamTableWriter(
                session=db_session,
                stream_table_name="tick_stream"  # 流表名称，需与creat_stream_table.dos中一致
            )
        except Exception as e:
            print(f"✗ 流表写入器创建失败: {e}")
            writer = None
    
    # 3. 创建数据转换器
    converter = TickDataConverter()
    
    # 4. 创建过滤配置（可选，使用默认配置则传None）
    # 默认配置：只采集 IM2603.CFFEX
    filter_config = FilterConfig(
        symbols=["IM2603"],           # 可以添加多个symbol，如 ["IM2603", "IM2604"]
        exchanges=[Exchange.CFFEX]   # 可以添加多个exchange
    )
    # 如果要采集所有tick，可以设置为：
    # filter_config = None  # 或 FilterConfig(symbols=None, exchanges=None)
    
    # 5. 手动创建数据采集引擎（因为需要传入额外参数）
    # 传入转换器和写入器（如果可用）
    collector_engine = TickCollectorEngine(
        main_engine=main_engine,
        event_engine=event_engine,
        converter=converter,
        writer=writer,              # 如果db_session为None，writer也为None，则只采集不写入
        filter_config=filter_config,
        enable_statistics=True,     # 启用统计功能
        debug=False                 # 关闭调试模式（生产环境）
    )
    
    # 手动注册引擎到main_engine（这样main_engine可以管理它）
    main_engine.engines[collector_engine.engine_name] = collector_engine
    
    # ============================================
    # 启动主窗口
    # ============================================
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()
    
    # 程序退出时，可以打印统计信息（可选）
    if collector_engine:
        stats = collector_engine.get_statistics()
        print(f"\n数据采集统计:")
        print(f"  - 采集tick数: {stats['tick_count']}")
        print(f"  - 过滤tick数: {stats['filtered_count']}")
    
    if writer:
        write_stats = writer.get_statistics()
        print(f"\n数据写入统计:")
        print(f"  - 写入次数: {write_stats['write_count']}")
        print(f"  - 错误次数: {write_stats['error_count']}")
        print(f"  - 成功率: {write_stats['success_rate']:.2%}")

if __name__ == "__main__":
    main()