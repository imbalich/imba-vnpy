"""
自定义CTP Gateway - 继承重写方式修复GFEX交易所KeyError问题
继承vnpy的CtpGateway和CtpTdApi，只重写onRspQryInstrument方法
"""

import logging
from datetime import datetime
from vnpy.gateway.ctp.ctp_gateway import (
    CtpGateway,
    CtpTdApi,
    CtpMdApi,  # 需要导入CtpMdApi类
    EXCHANGE_CTP2VT,
    PRODUCT_CTP2VT,
    OPTIONTYPE_CTP2VT,
    symbol_exchange_map,
    symbol_name_map,
    symbol_size_map,
)
from vnpy.trader.constant import Exchange, Product
from vnpy.trader.object import ContractData


class FixedCtpTdApi(CtpTdApi):
    """
    修复后的CTP交易API
    重写onRspQryInstrument方法，添加GFEX交易所的异常处理
    """
    
    def onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool):
        """
        回调函数：查询合约信息
        修复：添加GFEX交易所的异常处理，跳过不支持的交易所
        """
        # 检查是否有错误
        if error.get("ErrorID", 0):
            self.gateway.write_error("查询合约错误", error)
            if last:
                self.contract_inited = True
                self.gateway.write_log("合约信息查询完成")
            return
        
        # 获取产品类型
        product = PRODUCT_CTP2VT.get(data.get("ProductClass"), None)
        if not product:
            # 如果没有产品类型，直接返回
            if last:
                self.contract_inited = True
                self.gateway.write_log("合约信息查询完成")
            return
        
        # 获取交易所ID（关键修复点）
        exchange_id = data.get("ExchangeID", "")
        
        # 检查交易所是否支持（关键修复）
        if exchange_id not in EXCHANGE_CTP2VT:
            # 跳过不支持的交易所（如GFEX），只记录警告，不抛出异常
            # logging.warning(
            #     f"跳过不支持的交易所: {exchange_id} "
            #     f"(合约: {data.get('InstrumentID', 'Unknown')})"
            # )
            # 继续处理，不中断程序
            if last:
                self.contract_inited = True
                self.gateway.write_log("合约信息查询完成")
                
                # 处理待处理的订单和成交数据
                for order_data in self.order_data:
                    self.onRtnOrder(order_data)
                self.order_data.clear()
                
                for trade_data in self.trade_data:
                    self.onRtnTrade(trade_data)
                self.trade_data.clear()
            return
        
        # 获取交易所枚举
        try:
            exchange = EXCHANGE_CTP2VT[exchange_id]
        except KeyError:
            # 双重保险：如果还是找不到，记录日志并跳过
            logging.warning(
                f"交易所映射失败: {exchange_id} "
                f"(合约: {data.get('InstrumentID', 'Unknown')})"
            )
            if last:
                self.contract_inited = True
                self.gateway.write_log("合约信息查询完成")
                
                for order_data in self.order_data:
                    self.onRtnOrder(order_data)
                self.order_data.clear()
                
                for trade_data in self.trade_data:
                    self.onRtnTrade(trade_data)
                self.trade_data.clear()
            return
        
        # 创建合约对象（原逻辑）
        contract = ContractData(
            symbol=data["InstrumentID"],
            exchange=exchange,
            name=data["InstrumentName"],
            product=product,
            size=data["VolumeMultiple"],
            pricetick=data["PriceTick"],
            gateway_name=self.gateway_name
        )
        
        # 期权相关处理（保持原逻辑）
        if contract.product == Product.OPTION:
            # Remove C/P suffix of CZCE option product name
            if contract.exchange == Exchange.CZCE:
                contract.option_portfolio = data["ProductID"][:-1]
            else:
                contract.option_portfolio = data["ProductID"]
            
            contract.option_underlying = data["UnderlyingInstrID"]
            contract.option_type = OPTIONTYPE_CTP2VT.get(data["OptionsType"], None)
            contract.option_strike = data["StrikePrice"]
            contract.option_index = str(data["StrikePrice"])
            contract.option_expiry = datetime.strptime(data["ExpireDate"], "%Y%m%d")
        
        # 推送合约信息
        self.gateway.on_contract(contract)
        
        # 更新映射表
        symbol_exchange_map[contract.symbol] = contract.exchange
        symbol_name_map[contract.symbol] = contract.name
        symbol_size_map[contract.symbol] = contract.size
        
        # 查询完成
        if last:
            self.contract_inited = True
            self.gateway.write_log("合约信息查询成功")
            
            # 处理待处理的订单和成交数据
            for order_data in self.order_data:
                self.onRtnOrder(order_data)
            self.order_data.clear()
            
            for trade_data in self.trade_data:
                self.onRtnTrade(trade_data)
            self.trade_data.clear()


class FixedCtpGateway(CtpGateway):
    """
    修复后的CTP Gateway
    使用自定义的FixedCtpTdApi替代原来的CtpTdApi
    """
    
    def __init__(self, event_engine):
        """初始化，使用自定义的TdApi"""
        # 先调用父类初始化（但不创建TdApi和MdApi）
        from vnpy.trader.gateway import BaseGateway
        BaseGateway.__init__(self, event_engine, "CTP")
        
        # 使用自定义的TdApi和原来的MdApi
        self.td_api = FixedCtpTdApi(self)
        self.md_api = CtpMdApi(self)  # 使用原来的MdApi类创建实例
        
        # 初始化查询相关（从父类复制）
        self.count = 0
        self.query_functions = []
        
        print("✓ FixedCtpGateway 初始化完成（已修复GFEX交易所KeyError问题）")