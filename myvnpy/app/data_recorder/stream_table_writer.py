"""
流表写入器（高频交易优化版）
职责：将转换后的数据立即写入DolphinDB流表，记录write_start_timestamp
设计原则：每条tick立即写入，不批量，最小延迟
"""

import pandas as pd
import dolphindb as ddb
from typing import Dict, Optional
from .dolphindb_session import DolphinDBSession
from .tick_collector_engine import get_nanosecond_timestamp


class StreamTableWriter:
    """
    流表写入器（高频交易优化版）

    职责：
    1. 记录write_start_timestamp（纳秒级）
    2. 立即将数据写入DolphinDB流表（每条tick单独写入）
    3. 处理写入错误和重试
    4. 优化写入性能，最小化延迟

    设计原则：
    - 每条tick立即写入流表，不批量
    - 数据进入流表的速度要快
    - DolphinDB端的批量持久化（订阅机制）是可接受的

    不负责：
    - 数据转换（由TickDataConverter负责）
    - 连接管理（由DolphinDBSession负责）
    - 持久化（由DolphinDB订阅机制负责）

    写入方式（按优先级）：
    1. TableAppender - 高性能异步写入（推荐）
    2. tableInsert - 同步写入（备选）
    """

    def __init__(
        self,
        session: DolphinDBSession,
        stream_table_name: str = "tick_stream"
    ):
        """
        初始化流表写入器

        Args:
            session: DolphinDB会话对象
            stream_table_name: 流表名称
        """
        self.session = session
        self.stream_table_name = stream_table_name

        # 写入器（优先使用TableAppender）
        self.appender: Optional[ddb.TableAppender] = None

        # 写入统计（可选，用于监控）
        self.write_count = 0
        self.error_count = 0

        # 初始化写入器
        self._init_writer()

    def _init_writer(self):
        """初始化写入器"""
        # 对于高频交易场景，直接使用run方法执行tableInsert脚本
        # 这种方法简单可靠，避免TableAppender的复杂性
        self.appender = None  # 不使用TableAppender

        print("✓ StreamTableWriter 初始化完成（高频交易模式）")
        print(f"  - 流表名称: {self.stream_table_name}")
        print("  - 使用run方法执行tableInsert进行同步写入")
        print("  - 持久化: 由DolphinDB订阅机制批量处理")
    
    def write(self, data: Dict) -> bool:
        """
        立即写入单条数据到流表（高频交易优化）

        Args:
            data: 转换后的数据字典（包含receive_timestamp，write_start_timestamp为None）

        Returns:
            bool: True表示写入成功，False表示写入失败

        流程：
        1. 记录write_start_timestamp（纳秒级）
        2. 更新数据字典中的write_start_timestamp字段
        3. 立即写入流表（使用TableAppender或tableInsert）

        注意：
        - 每条tick单独写入，不批量
        - 数据立即进入流表，延迟最小
        - DolphinDB端的批量持久化由订阅机制处理（batchSize=100, throttle=0.1）
        """
        try:
            # 记录开始写入时间戳（关键时间点2：开始写入流表时刻，纳秒级精度）
            write_start_timestamp_ns = get_nanosecond_timestamp()

            # 更新数据字典中的write_start_timestamp字段（原地修改，避免复制）
            data["write_start_timestamp"] = write_start_timestamp_ns

            # 创建DataFrame（单行）
            df = pd.DataFrame([data])

            # datetime列现在已经是字符串格式（由TickDataConverter处理），无需额外处理

            # 使用run方法执行tableInsert（同步写入，高频场景优化）
            # 将DataFrame上传到DolphinDB，然后执行tableInsert
            # 注意：在tableInsert执行前记录write_end_timestamp，反映实际写入时刻
            self.session.session.upload({"tick_data": df})

            # 记录写入结束时间戳（关键时间点3：tableInsert执行前一刻，纳秒级精度）
            # 这样记录的是最接近实际写入流表的时间
            write_end_timestamp_ns = get_nanosecond_timestamp()

            script = f"""
            tableInsert({self.stream_table_name},
                tick_data.symbol[0],
                tick_data.exchange[0],
                tick_data.trade_date[0],
                temporalParse(tick_data.datetime[0], "yyyy-MM-dd HH:mm:ss.SSS"),
                tick_data.name[0],
                tick_data.last_price[0],
                tick_data.open_price[0],
                tick_data.high_price[0],
                tick_data.low_price[0],
                tick_data.pre_close[0],
                tick_data.limit_up[0],
                tick_data.limit_down[0],
                tick_data.volume[0],
                tick_data.last_volume[0],
                tick_data.open_interest[0],
                tick_data.bid_price_1[0],
                tick_data.bid_volume_1[0],
                tick_data.ask_price_1[0],
                tick_data.ask_volume_1[0],
                tick_data.gateway_name[0],
                nanotimestamp(tick_data.receive_timestamp[0]),
                nanotimestamp(tick_data.write_start_timestamp[0]),
                nanotimestamp({write_end_timestamp_ns})
            )
            """
            self.session.run(script)

            # 更新统计
            self.write_count += 1

            # 每100条打印统计（高频场景下减少输出）
            if self.write_count % 100 == 0:
                print(f"✓ 已写入 {self.write_count} 条tick数据到DolphinDB")

            return True

        except Exception as e:
            self.error_count += 1
            print(f"✗ 写入流表失败: {e}")
            # 调试时启用堆栈跟踪
            import traceback
            traceback.print_exc()
            return False
    
    def get_statistics(self) -> Dict:
        """
        获取写入统计信息

        Returns:
            dict: 包含写入次数、错误次数等统计信息
        """
        return {
            "write_count": self.write_count,
            "error_count": self.error_count,
            "success_rate": (self.write_count - self.error_count) / max(self.write_count, 1)
        }
