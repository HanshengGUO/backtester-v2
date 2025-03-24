import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from fee_rate_provider import FeeRateProvider

class TradingSignalBase(ABC):
    """
    交易信号基类，所有交易信号都继承自此类。
    提供获取入场和出场信号的接口。
    """
    
    def __init__(self):
        """
        初始化交易信号类
        """
        pass
    
    @abstractmethod
    def get_entry_signal(self, data):
        """
        获取入场信号
        
        Args:
            data (pd.DataFrame): 包含exchange_timestamp, swap_price, spot_price的数据框
            
        Returns:
            float: 信号强度，正值表示做多信号，负值表示做空信号，0表示无信号
        """
        pass
    
    @abstractmethod
    def get_exit_signal(self, data, position_type):
        """
        获取出场信号
        
        Args:
            data (pd.DataFrame): 包含exchange_timestamp, swap_price, spot_price的数据框
            position_type (str): 当前持仓类型，'long'或'short'
            
        Returns:
            float: 信号强度，正值表示平仓信号，0表示不平仓
        """
        pass


class TradingSignalFirst(TradingSignalBase):
    """
    第一个交易信号实现，模仿strategy.py中的策略
    """
    
    def __init__(self, inst_id="BTC-USDT-SWAP", exchange="okex"):
        """
        初始化交易信号类
        
        Args:
            inst_id (str): 交易对ID，默认为"BTC-USDT-SWAP"
            exchange (str): 交易所，默认为"okex"
        """
        super().__init__()
        
        # 阈值设置
        # 普通阈值
        self.short_in_threshold = 1.000758
        self.short_out_threshold = 0.999999
        self.long_in_threshold = 0.999000
        self.long_out_threshold = 1.000001
        
        # 正费率阈值 (应该做空，不应该做多)
        self.short_in_threshold_near_positive_fee = 1.000500
        self.short_out_threshold_near_positive_fee = 0.999750
        self.long_in_threshold_near_positive_fee = 0.998750
        self.long_out_threshold_near_positive_fee = 0.999750
        
        # 负费率阈值 (应该做多，不应该做空)
        self.short_in_threshold_near_negative_fee = 1.001000
        self.short_out_threshold_near_negative_fee = 1.000250
        self.long_in_threshold_near_negative_fee = 0.999250
        self.long_out_threshold_near_negative_fee = 1.000250
        
        # 初始化资金费率提供器
        self.fee_provider = FeeRateProvider(exchange=exchange)
        self.inst_id = inst_id
    
    def _is_near_settlement(self, timestamp):
        """
        检查是否接近资金费率结算时间（每8小时结算一次，接近时为True）
        
        Args:
            timestamp (int): 时间戳
            
        Returns:
            bool: 是否接近结算时间
        """
        # 每8小时结算一次，8小时=28800秒，如果距离结算时间小于800秒，则认为接近结算时间
        return timestamp % 28800 > 28000
    
    def _get_next_funding(self, timestamp):
        """
        获取下一个资金费率信息
        
        Args:
            timestamp (int): 当前时间戳
            
        Returns:
            tuple: (时间戳, 费率) 或 None
        """
        return self.fee_provider.get_next_funding_rate(timestamp, self.inst_id)
    
    def get_entry_signal(self, data):
        """
        获取入场信号
        
        Args:
            data (pd.DataFrame): 包含exchange_timestamp, swap_price, spot_price的数据框
            
        Returns:
            int: 1表示做多信号，-1表示做空信号，0表示无信号
        """
        # 获取最新数据
        latest_data = data.iloc[-1]
        
        timestamp = latest_data['exchange_timestamp']
        swap_price = latest_data['swap_price']
        spot_price = latest_data['spot_price']
        
        # 计算比率
        ratio_short = swap_price / spot_price  # swap bid / spot ask
        ratio_long = swap_price / spot_price   # swap ask / spot bid
        
        # 检查是否接近费率结算时间
        is_near_settlement = self._is_near_settlement(timestamp)
        
        # 获取下一个资金费率
        next_funding = self._get_next_funding(timestamp)
        
        if is_near_settlement and next_funding:
            _, funding_rate = next_funding
            if funding_rate > 0:  # 正费率，应该做空
                if ratio_short > self.short_in_threshold_near_positive_fee:
                    return -1  # 做空信号
            else:  # 负费率，应该做多
                if ratio_long < self.long_in_threshold_near_negative_fee:
                    return 1  # 做多信号
        else:  # 不在费率结算时间附近，使用普通阈值
            if ratio_short > self.short_in_threshold:
                return -1  # 做空信号
            if ratio_long < self.long_in_threshold:
                return 1  # 做多信号
        
        return 0  # 无信号
    
    def get_exit_signal(self, data, position_type):
        """
        获取出场信号
        
        Args:
            data (pd.DataFrame): 包含exchange_timestamp, swap_price, spot_price的数据框
            position_type (str): 当前持仓类型，'long'或'short'
            
        Returns:
            int: 1表示平仓信号，0表示不平仓
        """
        # 获取最新数据
        latest_data = data.iloc[-1]
        
        timestamp = latest_data['exchange_timestamp']
        swap_price = latest_data['swap_price']
        spot_price = latest_data['spot_price']
        
        # 检查是否接近费率结算时间
        is_near_settlement = self._is_near_settlement(timestamp)
        
        # 获取下一个资金费率
        next_funding = self._get_next_funding(timestamp)
        
        if position_type == 'short':
            # 做空出场条件
            ratio = swap_price / spot_price  # swap ask / spot bid
            
            threshold = self.short_out_threshold_near_positive_fee if is_near_settlement and next_funding and next_funding[1] > 0 else self.short_out_threshold
            
            if ratio < threshold:
                return 1  # 平空仓信号
        
        elif position_type == 'long':
            # 做多出场条件
            ratio = swap_price / spot_price  # swap bid / spot ask
            
            threshold = self.long_out_threshold_near_negative_fee if is_near_settlement and next_funding and next_funding[1] < 0 else self.long_out_threshold
            
            if ratio > threshold:
                return 1  # 平多仓信号
        
        return 0  # 不平仓 