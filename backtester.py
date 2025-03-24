import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime
import matplotlib.pyplot as plt
from tqdm import tqdm
import concurrent.futures
import multiprocessing

from trading_strategies import TradingStratsBase
from fee_rate_provider import FeeRateProvider
from market_data_provider import MarketDataProvider

class Backtester:
    """
    回测系统，用于回测交易策略
    """
    
    def __init__(self, strategy, instruments, dates, logger=None, window_size=10, interval_ms=1000, data_type="Depth"):
        """
        初始化回测系统
        
        Args:
            strategy (TradingStratsBase): 交易策略
            instruments (dict): 交易对配置，包含swap和spot的配置
            dates (list): 回测日期列表，格式为 YYYY-MM-DD
            logger (logging.Logger, optional): 日志记录器
            window_size (int): 数据窗口大小，传递给策略的历史数据条数
            interval_ms (int): 采样间隔，单位毫秒，默认1000ms
            data_type (str): 数据类型，"Depth"或"Fast"
        """
        self.strategy = strategy
        self.instruments = instruments
        self.dates = dates
        self.logger = logger or logging.getLogger(__name__)
        self.window_size = window_size
        self.interval_ms = interval_ms
        self.data_type = data_type
        
        self.results = {
            'dates': [],
            'pnl': [],
            'cumulative_pnl': [],
            'capital': [],
            'trades': [],
            'win_rate': []
        }
    
    def _preprocess_data(self, df):
        """
        预处理数据，将从MarketDataProvider获取的数据转换为策略需要的格式
        
        Args:
            df (pd.DataFrame): 原始数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        # 根据数据类型选择处理方式
        if self.data_type == "Depth":
            # 处理深度数据，计算中间价
            if isinstance(df, pd.DataFrame):
                # 如果是pandas DataFrame
                processed_df = pd.DataFrame({
                    'exchange_timestamp': df['timestamp'],
                    'swap_price': (df['swap_bid1'] + df['swap_ask1']) / 2,
                    'spot_price': (df['spot_bid1'] + df['spot_ask1']) / 2
                })
            else:
                # 如果是polars DataFrame，先转换为pandas
                processed_df = pd.DataFrame({
                    'exchange_timestamp': df['timestamp'].to_numpy(),
                    'swap_price': ((df['swap_bid1'] + df['swap_ask1']) / 2).to_numpy(),
                    'spot_price': ((df['spot_bid1'] + df['spot_ask1']) / 2).to_numpy()
                })
        elif self.data_type == "Fast":
            # 处理Fast数据
            if isinstance(df, pd.DataFrame):
                processed_df = pd.DataFrame({
                    'exchange_timestamp': df['timestamp'],
                    'swap_price': (df['swap_bid1'] + df['swap_ask1']) / 2,
                    'spot_price': (df['spot_bid1'] + df['spot_ask1']) / 2
                })
            else:
                processed_df = pd.DataFrame({
                    'exchange_timestamp': df['timestamp'].to_numpy(),
                    'swap_price': ((df['swap_bid1'] + df['swap_ask1']) / 2).to_numpy(),
                    'spot_price': ((df['spot_bid1'] + df['spot_ask1']) / 2).to_numpy()
                })
        else:
            raise ValueError(f"不支持的数据类型: {self.data_type}")
        
        return processed_df
    
    def run_single_day(self, date_str, debug_mode=False):
        """
        运行单日回测
        
        Args:
            date_str (str): 日期字符串，格式为 YYYY-MM-DD
            debug_mode (bool): 是否开启调试模式
            
        Returns:
            float: 当日盈亏
        """
        self.logger.info(f"开始回测 {date_str}...")
        
        # 创建市场数据提供者
        provider = MarketDataProvider(
            instruments=self.instruments,
            date=date_str,
            hour_offset=0,  # 从0点开始
            data_type=self.data_type
        )
        
        try:
            # 加载数据
            if self.data_type == "Depth":
                df_raw = provider.read_all_depth_by_interval(interval_ms=self.interval_ms, k=3)
            elif self.data_type == "Fast":
                df_raw = provider.read_all_fast_data()
            else:
                raise ValueError(f"不支持的数据类型: {self.data_type}")
            
            # 预处理数据
            df = self._preprocess_data(df_raw)
            
            if df.empty:
                self.logger.warning(f"{date_str} 数据为空")
                return 0.0
            
            # 记录起始资金
            start_capital = self.strategy.capital
            
            # 逐条处理数据
            all_trades = []
            for i in range(self.window_size, len(df)):
                # 获取数据窗口
                window = df.iloc[i-self.window_size:i+1]
                
                # 处理数据
                trades = self.strategy.process_data(window)
                
                # 记录交易
                for trade in trades:
                    all_trades.append((df.iloc[i]['exchange_timestamp'], *trade))
                    if debug_mode:
                        action, position_type, amount, swap_price, spot_price = trade
                        self.logger.info(f"{action} {position_type} {amount} BTC @ swap:{swap_price:.2f} spot:{spot_price:.2f}")
            
            # 强制平仓所有未平持仓
            if self.strategy.positions:
                self.logger.info(f"收盘时强制平仓 {len(self.strategy.positions)} 个持仓")
                
                # 获取最后的价格
                last_data = df.iloc[-1]
                last_timestamp = last_data['exchange_timestamp']
                last_swap_price = last_data['swap_price']
                last_spot_price = last_data['spot_price']
                
                # 创建包含最后一条数据的窗口
                last_window = df.iloc[-self.window_size:]
                
                # 平掉所有持仓
                for position in list(self.strategy.positions):
                    # 计算盈亏
                    pnl = position.calculate_pnl(last_swap_price, last_spot_price)
                    
                    # 计算手续费
                    fee = (position.swap_price + position.spot_price + last_swap_price + last_spot_price) * position.amount * self.strategy.fee_rate
                    
                    # 更新资金
                    self.strategy.capital += pnl - fee
                    
                    # 更新统计信息
                    self.strategy.total_pnl += pnl
                    self.strategy.total_fee += fee
                    self.strategy.trade_count += 1
                    if pnl > 0:
                        self.strategy.win_count += 1
                    
                    # 记录交易
                    all_trades.append((last_timestamp, 'close', position.position_type, position.amount, last_swap_price, last_spot_price))
                    
                    # 将持仓移至已平仓列表
                    self.strategy.closed_positions.append(position)
                    self.strategy.positions.remove(position)
                    
                    if debug_mode:
                        self.logger.info(f"强制平仓 {position.position_type} {position.amount} BTC @ swap:{last_swap_price:.2f} spot:{last_spot_price:.2f}, PnL: {pnl:.6f}")
            
            # 计算日盈亏
            day_pnl = self.strategy.capital - start_capital
            
            # 计算胜率
            win_rate = self.strategy.win_count / max(1, self.strategy.trade_count)
            
            # 更新结果
            self.results['dates'].append(date_str)
            self.results['pnl'].append(day_pnl)
            self.results['cumulative_pnl'].append(self.strategy.total_pnl)
            self.results['capital'].append(self.strategy.capital)
            self.results['trades'].append(all_trades)
            self.results['win_rate'].append(win_rate)
            
            self.logger.info(f"{date_str} 交易日结束, 日盈亏: {day_pnl:.6f}, 总盈亏: {self.strategy.total_pnl:.6f}, 当前资金: {self.strategy.capital:.6f}")
            
            return day_pnl
        
        finally:
            # 确保关闭数据提供者
            provider.close()
    
    def run(self, debug_mode=False, parallel=True):
        """
        运行回测
        
        Args:
            debug_mode (bool): 是否开启调试模式
            parallel (bool): 是否使用并行计算
            
        Returns:
            dict: 回测结果
        """
        self.logger.info(f"开始回测，日期范围: {self.dates[0]} 至 {self.dates[-1]}")
        
        # 清空结果
        self.results = {
            'dates': [],
            'pnl': [],
            'cumulative_pnl': [],
            'capital': [],
            'trades': [],
            'win_rate': []
        }
        
        # 记录起始资金
        initial_capital = self.strategy.capital
        
        if parallel and not debug_mode and len(self.dates) > 1:
            # 使用并行计算
            max_workers = min(16, multiprocessing.cpu_count())
            
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self.run_single_day, date, debug_mode) for date in self.dates]
                for future in tqdm(concurrent.futures.as_completed(futures), total=len(self.dates), desc="回测进度"):
                    _ = future.result()
        else:
            # 逐日回测
            for date in tqdm(self.dates, desc="回测进度"):
                self.run_single_day(date, debug_mode)
        
        # 计算回测统计数据
        total_pnl = self.strategy.total_pnl
        total_fee = self.strategy.total_fee
        total_funding_fee = self.strategy.total_funding_fee
        
        # 计算年化收益率
        trading_days = len(self.dates)
        annual_return = (total_pnl / initial_capital) * (252 / trading_days) * 100
        
        # 计算胜率
        win_rate = self.strategy.win_count / max(1, self.strategy.trade_count)
        
        self.logger.info(f"\n回测结束")
        self.logger.info(f"总盈亏: {total_pnl:.6f} USDT")
        self.logger.info(f"总手续费: {total_fee:.6f} USDT")
        self.logger.info(f"总资金费: {total_funding_fee:.6f} USDT")
        self.logger.info(f"净盈亏: {total_pnl - total_fee - total_funding_fee:.6f} USDT")
        self.logger.info(f"年化收益率: {annual_return:.2f}%")
        self.logger.info(f"交易次数: {self.strategy.trade_count}")
        self.logger.info(f"胜率: {win_rate:.2f}")
        self.logger.info(f"总本金: {initial_capital:.2f} USDT")
        self.logger.info(f"最终资金: {self.strategy.capital:.2f} USDT")
        
        return {
            'total_pnl': total_pnl,
            'total_fee': total_fee,
            'total_funding_fee': total_funding_fee,
            'net_pnl': total_pnl - total_fee - total_funding_fee,
            'annual_return': annual_return,
            'trade_count': self.strategy.trade_count,
            'win_rate': win_rate,
            'initial_capital': initial_capital,
            'final_capital': self.strategy.capital,
            'daily_results': self.results
        }
    
    def plot_results(self, save_path=None):
        """
        绘制回测结果
        
        Args:
            save_path (str, optional): 图表保存路径
        """
        if not self.results['dates']:
            self.logger.warning("没有回测结果可供绘制")
            return
        
        # 创建图表
        fig, axes = plt.subplots(3, 1, figsize=(12, 15), gridspec_kw={'height_ratios': [3, 1, 1]})
        
        # 绘制资金曲线
        axes[0].plot(self.results['dates'], self.results['capital'], label='资金曲线', color='blue')
        axes[0].set_title('回测结果 - 资金曲线')
        axes[0].set_ylabel('资金 (USDT)')
        axes[0].legend()
        axes[0].grid(True)
        
        # 绘制日盈亏
        colors = ['green' if pnl >= 0 else 'red' for pnl in self.results['pnl']]
        axes[1].bar(self.results['dates'], self.results['pnl'], color=colors)
        axes[1].set_title('每日盈亏')
        axes[1].set_ylabel('盈亏 (USDT)')
        axes[1].grid(True)
        
        # 绘制胜率
        axes[2].plot(self.results['dates'], self.results['win_rate'], label='胜率', color='orange')
        axes[2].set_title('累计胜率')
        axes[2].set_ylabel('胜率')
        axes[2].set_ylim([0, 1])
        axes[2].grid(True)
        
        # 调整布局
        plt.tight_layout()
        
        # 保存图表
        if save_path:
            plt.savefig(save_path)
            self.logger.info(f"回测结果图表已保存至 {save_path}")
        
        plt.show()


# 示例用法
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("Backtester")
    
    # 交易对配置
    instruments = {
        "swap": {
            "name": "okex_swap_btcusdt",
            "data_path": "/data/l2/okex/okex_swap_btcusdt",
        },
        "spot": {
            "name": "okex_spot_btc_usdt",
            "data_path": "/data/l2/okex/okex_spot_btc_usdt",
        }
    }
    
    # 创建交易策略
    from trading_strategies import TradingStratsFirst
    strategy = TradingStratsFirst(
        capital=10000,  # 10000 USDT
        max_positions=2,
        fee_rate=0.00015,  # Taker费率
        funding_fee_enabled=True
    )
    
    # 创建回测系统
    backtester = Backtester(
        strategy=strategy,
        instruments=instruments,
        dates=["2023-01-01", "2023-01-02", "2023-01-03"],
        logger=logger,
        window_size=10,
        interval_ms=1000,
        data_type="Depth"
    )
    
    # 运行回测
    results = backtester.run(debug_mode=True, parallel=False)
    
    # 绘制结果
    backtester.plot_results(save_path="backtest_results.png") 