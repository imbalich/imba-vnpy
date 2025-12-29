from datetime import datetime

from vnpy.trader.constant import Interval
from vnpy.app.cta_strategy.strategies.boll_channel_strategy import BollChannelStrategy
from vnpy.app.cta_strategy.backtesting import BacktestingEngine

engine = BacktestingEngine()
engine.set_parameters(
    vt_symbol="IM202505.CFFEX",
    interval=Interval.MINUTE,
    start=datetime(2023, 1, 1),
    end=datetime(2023, 5, 1),
    rate = 0.3/10000,
    slippage = 0.2,
    size = 300,
    pricetick = 0.2,
    capital=1_000_000,
    
)
engine.add_strategy(BollChannelStrategy, {})
engine.load_data()
engine.run_backtesting()
df = engine.calculate_result()
print(df)
engine.calculate_statistics()
engine.show_chart()