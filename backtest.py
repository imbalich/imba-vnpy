from datetime import datetime

from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.database import database_manager
from vnpy.trader.rqdata import rqdata_client
from vnpy.trader.object import HistoryRequest
from vnpy.app.cta_strategy.strategies.boll_channel_strategy import BollChannelStrategy
from vnpy.app.cta_strategy.backtesting import BacktestingEngine

# 回测参数
vt_symbol = "IM202601.CFFEX"
interval = Interval.MINUTE
start = datetime(2025, 12, 1)
end = datetime(2025, 12, 29)

# 检查数据库中是否有数据
symbol, exchange_str = vt_symbol.split(".")
exchange = Exchange(exchange_str)
existing_data = database_manager.load_bar_data(symbol, exchange, interval, start, end)

if not existing_data:
    print("数据库中没有数据，尝试从 RQData 下载...")

    # 初始化 RQData
    if not rqdata_client.inited:
        print("正在初始化 RQData...")
        success = rqdata_client.init()
        if not success:
            print(
                "警告: RQData 初始化失败，请检查配置文件中是否有 rqdata.username 和 rqdata.password"
            )
            print("请先运行 download_data.py 下载数据，或配置 RQData 账号")

    # 尝试从 RQData 下载数据
    if rqdata_client.inited:
        req = HistoryRequest(
            symbol=symbol, exchange=exchange, interval=interval, start=start, end=end
        )
        data = rqdata_client.query_history(req)

        if data:
            print(f"从 RQData 获取到 {len(data)} 条数据，正在保存到数据库...")
            database_manager.save_bar_data(data)
            print("数据下载完成！")
        else:
            print("错误: 未能从 RQData 获取数据")
            print("请检查:")
            print("1. RQData 账号是否正确配置")
            print("2. 合约代码是否正确")
            print("3. 时间范围内是否有数据")
            exit(1)
    else:
        print("请先运行 download_data.py 下载数据")
        exit(1)
else:
    print(f"数据库中找到 {len(existing_data)} 条历史数据")

# 开始回测
engine = BacktestingEngine()
engine.set_parameters(
    vt_symbol=vt_symbol,
    interval=interval,
    start=start,
    end=end,
    rate=0.3 / 10000,
    slippage=0.2,
    size=1,
    pricetick=0.2,
    capital=1_000_000,
)
engine.add_strategy(BollChannelStrategy, {})
engine.load_data()
engine.run_backtesting()
df = engine.calculate_result()
print(df)
engine.calculate_statistics()
engine.show_chart()
