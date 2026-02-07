"""
订阅DolphinDB因子输出流表，生成信号并推送到RabbitMQ
"""
import dolphindb as ddb
import pandas as pd
from datetime import datetime
import time
import uuid
import sys
from pathlib import Path

# 获取项目根目录（当前文件向上两级：script/python -> script -> 项目根目录）
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from myvnpy.mq.connection import RabbitMQConnection
from myvnpy.mq.producer import SignalProducer
from myvnpy.signal.signal import TradingSignal, SignalType, SignalStrength


class FactorSubscriber:
    """因子订阅器"""
    
    def __init__(
        self, 
        ddb_host="localhost", 
        ddb_port=8848,
        mq_host="localhost",
        mq_port=5672
    ):
        # DolphinDB 连接
        self.ddb_session = ddb.Session()
        self.ddb_session.connect(ddb_host, ddb_port, "admin", "123456")
        self.ddb_session.enableStreaming(0)
        
        # RabbitMQ 生产者
        self.mq_connection = RabbitMQConnection(host=mq_host, port=mq_port)
        self.producer = SignalProducer(self.mq_connection)
        
        print("✓ 已连接 DolphinDB 和 RabbitMQ")
    
    def on_factor_data(self, data: pd.DataFrame):
        """因子数据回调"""
        for _, row in data.iterrows():
            symbol = row['symbol']
            factor_value = row['factorValue']
            
            # 生成信号
            signal = self.generate_signal(symbol, factor_value)
            
            if signal:
                # 发布到 RabbitMQ
                self.producer.publish(signal)
                print(f"[{datetime.now()}] 发布信号: {symbol} "
                      f"{signal.signal_type.value} 因子={factor_value:.4f}")
    
    def generate_signal(self, symbol: str, factor_value: float) -> TradingSignal:
        """根据因子值生成交易信号"""
        # 信号生成逻辑：> 0.7 做多，< 0.3 做空，其他不发信号
        if factor_value > 0.7:
            signal_type = SignalType.BUY
            strength = SignalStrength.STRONG
        elif factor_value < 0.3:
            signal_type = SignalType.SELL
            strength = SignalStrength.STRONG
        else:
            # 0.3 ~ 0.7 之间不发送信号
            return None
        
        return TradingSignal(
            signal_id=str(uuid.uuid4()),
            symbol=symbol,
            signal_type=signal_type,
            signal_strength=strength,
            factor_value=factor_value,
            volume=1
        )
    
    def subscribe(self, table_name="factor_combine_result"):
        """订阅因子流表"""
        self.ddb_session.subscribe(
            host="localhost",
            port=8848,
            handler=self.on_factor_data,
            tableName=table_name,
            actionName="signal_generator",
            offset=-1,
            resub=True,
            msgAsTable=True,
            batchSize=1,      # msgAsTable=True 时必须指定 batchSize
            throttle=0.01     # 节流时间 10ms
        )
        print(f"✓ 已订阅流表: {table_name}")
    
    def close(self):
        """关闭连接"""
        self.ddb_session.close()
        self.producer.close()


def main():
    subscriber = FactorSubscriber()
    
    try:
        subscriber.subscribe("factor_combine_result")
        print("等待因子数据，按 Ctrl+C 停止...")
        
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n停止...")
    finally:
        subscriber.close()


if __name__ == "__main__":
    main()