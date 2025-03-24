import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from trading_signals import TradingSignalBase, TradingSignalFirst
from fee_rate_provider import FeeRateProvider

class Position:
    """
    持仓类，用于记录当前持仓信息
    """
    
    def __init__(self, position_type, entry_time, swap_price, spot_price, amount, position_id=None):
        """
        初始化持仓
        
        Args:
            position_type (str): 持仓类型，'long'或'short'
            entry_time (int): 入场时间戳
            swap_price (float): 期货入场价格
            spot_price (float): 现货入场价格
            amount (float): 持仓数量，以BTC为单位
            position_id (int, optional): 持仓ID，用于标识唯一持仓
        """
        self.position_type = position_type  # 'long'或'short'
        self.entry_time = entry_time
        self.swap_price = swap_price
        self.spot_price = spot_price
        self.amount = amount  # 以BTC为单位
        self.position_id = position_id
        self.funding_payments = []  # 记录资金费支付，每项为 (timestamp, amount)
    
    def add_funding_payment(self, timestamp, amount):
        """
        添加资金费支付记录
        
        Args:
            timestamp (int): 时间戳
            amount (float): 支付金额，正值表示收到，负值表示支付
        """
        self.funding_payments.append((timestamp, amount))
    
    def calculate_pnl(self, exit_swap_price, exit_spot_price):
        """
        计算平仓时的盈亏
        
        Args:
            exit_swap_price (float): 期货出场价格
            exit_spot_price (float): 现货出场价格
            
        Returns:
            float: 盈亏金额，不包括手续费和资金费
        """
        if self.position_type == 'long':
            # 多仓: 卖出现货, 买入期货 -> 买入现货, 卖出期货
            return (exit_spot_price - self.spot_price + (self.swap_price - exit_swap_price)) * self.amount
        else:
            # 空仓: 买入现货, 卖出期货 -> 卖出现货, 买入期货
            return (self.spot_price - exit_spot_price + (exit_swap_price - self.swap_price)) * self.amount


class TradingStratsBase(ABC):
    """
    交易策略基类，所有交易策略都继承自此类
    """
    
    def __init__(self, capital, max_positions=2, fee_rate=0.00015, funding_fee_enabled=True):
        """
        初始化交易策略
        
        Args:
            capital (float): 初始资金，单位USDT
            max_positions (int): 最大持仓数
            fee_rate (float): 手续费率，Taker费率
            funding_fee_enabled (bool): 是否启用资金费计算
        """
        self.initial_capital = capital
        self.capital = capital
        self.max_positions = max_positions
        self.fee_rate = fee_rate
        self.funding_fee_enabled = funding_fee_enabled
        
        self.positions = []  # 当前持仓列表
        self.closed_positions = []  # 已平仓持仓列表
        self.next_position_id = 1  # 下一个持仓ID
        
        self.fee_provider = FeeRateProvider()
        
        # 统计信息
        self.total_pnl = 0.0
        self.total_fee = 0.0
        self.total_funding_fee = 0.0
        self.trade_count = 0
        self.win_count = 0
        
    def get_position_size(self):
        """
        获取每个持仓的资金大小
        
        Returns:
            float: 每个持仓的资金大小
        """
        return self.initial_capital / self.max_positions
    
    def can_open_position(self, position_type):
        """
        检查是否可以开仓
        
        Args:
            position_type (str): 持仓类型，'long'或'short'
            
        Returns:
            bool: 是否可以开仓
        """
        # 检查是否达到最大持仓数
        if len(self.positions) >= self.max_positions:
            return False
        
        # 子类可以重写此方法以实现更复杂的逻辑
        return True
    
    def calculate_trade_amount(self, swap_price, spot_price):
        """
        计算交易数量，以BTC为单位
        
        Args:
            swap_price (float): 期货价格
            spot_price (float): 现货价格
            
        Returns:
            float: 交易数量，以BTC为单位
        """
        position_size = self.get_position_size()
        max_price = max(swap_price, spot_price)
        return position_size / max_price
    
    def process_funding_fees(self, timestamp):
        """
        处理资金费用，在每个资金费结算时间调用
        
        Args:
            timestamp (int): 当前时间戳
        """
        if not self.funding_fee_enabled or not self.positions:
            return
        
        # 获取上一个资金费率
        prev_ts, prev_rate = self.fee_provider.get_prev_funding_rate(timestamp)
        
        if prev_ts and prev_ts == timestamp:
            # 为每个持仓支付/收取资金费
            for position in self.positions:
                if position.entry_time < timestamp:  # 只处理结算前开仓的持仓
                    # 计算资金费
                    funding_fee = 0.0
                    if position.position_type == 'long':
                        # 多仓收到负费率，支付正费率
                        funding_fee = -position.swap_price * position.amount * prev_rate
                    else:
                        # 空仓收到正费率，支付负费率
                        funding_fee = position.swap_price * position.amount * prev_rate
                    
                    # 更新资金
                    self.capital += funding_fee
                    self.total_funding_fee += funding_fee
                    
                    # 记录资金费支付
                    position.add_funding_payment(timestamp, funding_fee)
    
    @abstractmethod
    def process_data(self, data):
        """
        处理市场数据，决定交易行为
        
        Args:
            data (pd.DataFrame): 包含exchange_timestamp, swap_price, spot_price的数据框
            
        Returns:
            list: 执行的交易列表，每项为 (action, position_type, amount, swap_price, spot_price)
                  action为'open'或'close'
        """
        pass


class TradingStratsFirst(TradingStratsBase):
    """
    第一个交易策略实现，使用TradingSignalFirst产生信号
    """
    
    def __init__(self, capital, max_positions=2, fee_rate=0.00015, funding_fee_enabled=True, 
                 inst_id="BTC-USDT-SWAP", exchange="okex"):
        """
        初始化交易策略
        
        Args:
            capital (float): 初始资金，单位USDT
            max_positions (int): 最大持仓数
            fee_rate (float): 手续费率
            funding_fee_enabled (bool): 是否启用资金费计算
            inst_id (str): 交易对ID
            exchange (str): 交易所
        """
        super().__init__(capital, max_positions, fee_rate, funding_fee_enabled)
        
        # 创建交易信号对象
        self.signal = TradingSignalFirst(inst_id=inst_id, exchange=exchange)
        
        # 记录当前持仓类型，用于限制不同方向的持仓
        self.current_position_type = None
        
        # 记录最后一次入场时间
        self.last_entry_time = 0
        
        # 记录下一个资金费结算时间
        self.next_funding_time = None
    
    def can_open_position(self, position_type):
        """
        检查是否可以开仓
        
        Args:
            position_type (str): 持仓类型，'long'或'short'
            
        Returns:
            bool: 是否可以开仓
        """
        # 检查是否达到最大持仓数
        if len(self.positions) >= self.max_positions:
            return False
        
        # 检查是否已有相反方向的持仓
        if self.current_position_type is not None and self.current_position_type != position_type:
            return False
        
        return True
    
    def process_data(self, data):
        """
        处理市场数据，决定交易行为
        
        Args:
            data (pd.DataFrame): 包含exchange_timestamp, swap_price, spot_price的数据框
            
        Returns:
            list: 执行的交易列表，每项为 (action, position_type, amount, swap_price, spot_price)
                  action为'open'或'close'
        """
        if data.empty:
            return []
        
        # 获取最新数据
        latest_data = data.iloc[-1]
        timestamp = latest_data['exchange_timestamp']
        swap_price = latest_data['swap_price']
        spot_price = latest_data['spot_price']
        
        # 处理资金费
        self.process_funding_fees(timestamp)
        
        # 更新下一个资金费结算时间
        if self.next_funding_time is None or timestamp >= self.next_funding_time:
            next_funding = self.fee_provider.get_next_funding_rate(timestamp)
            if next_funding and next_funding[0]:
                self.next_funding_time = next_funding[0]
        
        trades = []
        
        # 检查是否有持仓需要平仓
        for position in list(self.positions):
            # 获取出场信号
            exit_signal = self.signal.get_exit_signal(data, position.position_type)
            
            if exit_signal > 0:
                # 平仓
                # 计算盈亏
                pnl = position.calculate_pnl(swap_price, spot_price)
                
                # 计算手续费
                fee = (position.swap_price + position.spot_price + swap_price + spot_price) * position.amount * self.fee_rate
                
                # 更新资金
                self.capital += pnl - fee
                
                # 更新统计信息
                self.total_pnl += pnl
                self.total_fee += fee
                self.trade_count += 1
                if pnl > 0:
                    self.win_count += 1
                
                # 记录交易
                trades.append(('close', position.position_type, position.amount, swap_price, spot_price))
                
                # 将持仓移至已平仓列表
                self.closed_positions.append(position)
                self.positions.remove(position)
                
                # 如果没有持仓了，重置当前持仓类型
                if not self.positions:
                    self.current_position_type = None
        
        # 如果没有持仓，检查是否有入场信号
        if len(self.positions) < self.max_positions:
            # 获取入场信号
            entry_signal = self.signal.get_entry_signal(data)
            
            # 如果有资金费结算时间，且当前时间接近结算时间，等待结算后再入场
            if self.next_funding_time and timestamp > self.next_funding_time - 800:
                entry_signal = 0
            
            # 检查是否有足够的时间间隔
            min_entry_interval = 3600  # 至少间隔1小时
            if timestamp - self.last_entry_time < min_entry_interval:
                entry_signal = 0
            
            if entry_signal != 0:
                position_type = 'long' if entry_signal > 0 else 'short'
                
                # 检查是否可以开仓
                if self.can_open_position(position_type):
                    # 计算交易数量
                    amount = self.calculate_trade_amount(swap_price, spot_price)
                    
                    # 创建新持仓
                    position = Position(
                        position_type=position_type,
                        entry_time=timestamp,
                        swap_price=swap_price,
                        spot_price=spot_price,
                        amount=amount,
                        position_id=self.next_position_id
                    )
                    self.next_position_id += 1
                    
                    # 计算手续费
                    fee = (swap_price + spot_price) * amount * self.fee_rate
                    
                    # 更新资金
                    self.capital -= fee
                    self.total_fee += fee
                    
                    # 添加持仓
                    self.positions.append(position)
                    
                    # 更新当前持仓类型
                    self.current_position_type = position_type
                    
                    # 更新最后入场时间
                    self.last_entry_time = timestamp
                    
                    # 记录交易
                    trades.append(('open', position_type, amount, swap_price, spot_price))
        
        return trades 