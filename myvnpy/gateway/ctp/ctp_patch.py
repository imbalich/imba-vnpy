"""
CTP Gateway补丁 - 修复GFEX交易所KeyError问题
在导入vnpy.gateway.ctp后执行此补丁
"""

import logging
# 直接从ctp_gateway模块导入，确保类已完全加载
from vnpy.gateway.ctp import ctp_gateway


def patch_ctp_gateway():
    """修补CTP Gateway的onRspQryInstrument方法"""
    
    # 从模块中获取类（注意：onRspQryInstrument在CtpTdApi中，不在CtpMdApi中）
    CtpTdApi = ctp_gateway.CtpTdApi
    EXCHANGE_CTP2VT = ctp_gateway.EXCHANGE_CTP2VT
    
    # 检查方法是否存在
    if not hasattr(CtpTdApi, 'onRspQryInstrument'):
        print("⚠ 警告: CtpTdApi.onRspQryInstrument 方法不存在，跳过补丁")
        return
    
    # 保存原始方法
    original_onRspQryInstrument = getattr(CtpTdApi, 'onRspQryInstrument')
    
    def fixed_onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool):
        """
        修复后的onRspQryInstrument方法
        添加GFEX交易所的异常处理
        """
        # 检查是否有错误
        if error.get("ErrorID", 0):
            self.gateway.write_log(f"查询合约错误: {error.get('ErrorMsg', '')}")
            if last:
                self.contract_inited = True
                self.gateway.write_log("合约信息查询完成")
            return
        
        # 获取交易所ID
        exchange_id = data.get("ExchangeID", "")
        
        # 检查交易所是否支持（关键修复）
        if exchange_id not in EXCHANGE_CTP2VT:
            # 跳过不支持的交易所（如GFEX），只记录警告，不抛出异常
            logging.warning(
                f"跳过不支持的交易所: {exchange_id} "
                f"(合约: {data.get('InstrumentID', 'Unknown')})"
            )
            # 继续处理，不中断程序
            if last:
                self.contract_inited = True
                self.gateway.write_log("合约信息查询完成")
            return
        
        # 调用原始方法处理支持的交易所
        try:
            return original_onRspQryInstrument(self, data, error, reqid, last)
        except KeyError as e:
            # 如果还是出现KeyError，记录日志但不中断程序
            logging.warning(
                f"处理合约时出错: {e} "
                f"(交易所: {exchange_id}, 合约: {data.get('InstrumentID', 'Unknown')})"
            )
            if last:
                self.contract_inited = True
                self.gateway.write_log("合约信息查询完成")
    
    # 替换方法
    CtpTdApi.onRspQryInstrument = fixed_onRspQryInstrument
    print("✓ CTP Gateway补丁已应用：已修复GFEX交易所KeyError问题")
