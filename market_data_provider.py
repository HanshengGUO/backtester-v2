from crypto import MarketDataReader, Fast, Depth
import os
from datetime import datetime, timedelta
import polars as pl
import numpy as np
import time

class MarketDataProvider:
    def __init__(self, instruments, date, hour_offset=0, data_type="Depth"):
        self.instruments = instruments
        self.date = date
        self.hour_offset = hour_offset
        self.data_type = data_type
        
        # 计算开始和结束时间
        self.start_datetime = datetime.strptime(date, "%Y-%m-%d") + timedelta(hours=hour_offset)
        self.end_datetime = self.start_datetime + timedelta(hours=24)
        
        # 获取所需文件路径
        self.start_date = self.start_datetime.strftime("%Y-%m-%d")
        self.end_date = self.end_datetime.strftime("%Y-%m-%d")
        
        # 初始化数据读取器
        self.swap_readers = self._init_readers("swap")
        self.spot_readers = self._init_readers("spot")
        
        if not self.swap_readers or not self.spot_readers:
            raise FileNotFoundError("无法初始化数据读取器，请检查文件路径")
        
        self.current_swap_reader_idx = 0
        self.current_spot_reader_idx = 0
        
        # 获取当前活动的读取器
        self.swap_reader = self.swap_readers[self.current_swap_reader_idx]
        self.spot_reader = self.spot_readers[self.current_spot_reader_idx]
        
        # 调整到起始时间
        self._seek_to_start_time()
        
        # 初始化第一条swap数据
        self.current_swap = self._read_next_valid_swap()
        
        # 记录最后一次读取的时间戳
        self.last_timestamp = None
        
    def _init_readers(self, market_type):
        data_path = self.instruments[market_type]["data_path"]
        readers = []
        
        # 检查开始日期的文件
        start_file = f"{data_path}/{self.start_date}.bin"
        if os.path.exists(start_file):
            readers.append(MarketDataReader(start_file))
        else:
            raise FileNotFoundError(f"无法找到文件: {start_file}")
        
        # 如果跨天，还需要检查结束日期的文件
        if self.start_date != self.end_date:
            end_file = f"{data_path}/{self.end_date}.bin"
            if os.path.exists(end_file):
                readers.append(MarketDataReader(end_file))
            else:
                raise FileNotFoundError(f"无法找到文件: {end_file}")
        
        return readers
        
    def _seek_to_start_time(self):
        # 计算目标时间戳 (开始时间对应的时间戳)
        target_ts = int(self.start_datetime.timestamp())
        
        # 将读取器移动到目标时间戳附近
        while self.swap_reader.peek() < target_ts and self.swap_reader.peek() != float("inf"):
            self.swap_reader.read()
            
        while self.spot_reader.peek() < target_ts and self.spot_reader.peek() != float("inf"):
            self.spot_reader.read()
        
    def _switch_to_next_reader_if_needed(self, reader_type):
        """尝试切换到下一个文件的读取器"""
        if reader_type == "swap":
            if self.current_swap_reader_idx < len(self.swap_readers) - 1 and self.swap_reader.peek() == float("inf"):
                self.current_swap_reader_idx += 1
                self.swap_reader = self.swap_readers[self.current_swap_reader_idx]
                return True
        elif reader_type == "spot":
            if self.current_spot_reader_idx < len(self.spot_readers) - 1 and self.spot_reader.peek() == float("inf"):
                self.current_spot_reader_idx += 1
                self.spot_reader = self.spot_readers[self.current_spot_reader_idx]
                return True
        return False
        
    def _read_next_valid_swap(self):
        while True:
            if self.swap_reader.peek() == float("inf"):
                # 尝试切换到下一个文件
                if not self._switch_to_next_reader_if_needed("swap"):
                    return None
            
            data = self.swap_reader.read()
            
            if self.data_type == "Depth" and isinstance(data, Depth):
                # 检查是否超过结束时间
                if data.timestamp > int(self.end_datetime.timestamp()):
                    return None
                return data
            elif self.data_type == "Fast" and isinstance(data, Fast):
                # 检查是否超过结束时间
                if data.timestamp > int(self.end_datetime.timestamp()):
                    return None
                return data
        
    def _read_next_valid_spot(self):
        while True:
            if self.spot_reader.peek() == float("inf"):
                # 尝试切换到下一个文件
                if not self._switch_to_next_reader_if_needed("spot"):
                    return None
            
            data = self.spot_reader.read()
            
            if self.data_type == "Depth" and isinstance(data, Depth):
                # 检查是否超过结束时间
                if data.timestamp > int(self.end_datetime.timestamp()):
                    return None
                return data
            elif self.data_type == "Fast" and isinstance(data, Fast):
                # 检查是否超过结束时间
                if data.timestamp > int(self.end_datetime.timestamp()):
                    return None
                return data
        
    def read_next(self):
        while True:
            # 读取spot数据
            spot = self._read_next_valid_spot()
            if spot is None:
                return None, None
                
            best_swap = None
            # 如果当前swap的时间戳已经大于spot，继续读取下一个spot
            if self.current_swap is None or self.current_swap.timestamp > spot.timestamp:
                continue
                
            # 找到最后一个timestamp <= spot timestamp的swap
            while self.current_swap is not None and self.current_swap.timestamp <= spot.timestamp:
                best_swap = self.current_swap.clone()
                self.current_swap = self._read_next_valid_swap()
                
            if best_swap is not None:
                return best_swap, spot
                
    def read_next_depth_by_interval(self, interval_ms=1000):
        """
        按指定的时间间隔(毫秒)读取下一条深度数据
        
        Args:
            interval_ms (int): 时间间隔，单位毫秒，默认1000ms(1秒)
            
        Returns:
            tuple: (swap深度数据, spot深度数据) 或 (None, None)表示结束
        """
        if self.data_type != "Depth":
            raise ValueError("当前数据类型不是Depth，请使用Depth类型初始化Provider")
            
        # 如果是第一次读取，直接返回第一条数据
        if self.last_timestamp is None:
            swap, spot = self.read_next()
            if swap is not None:
                self.last_timestamp = swap.timestamp
            return swap, spot
            
        # 计算目标时间戳（上次时间戳 + 间隔）
        target_timestamp = self.last_timestamp + interval_ms / 1000.0
        
        # 一直读取直到时间戳大于等于目标时间戳
        while True:
            swap, spot = self.read_next()
            
            # 如果数据结束，返回None
            if swap is None or spot is None:
                return None, None
                
            # 如果时间间隔大于设定值，更新last_timestamp并返回数据
            if swap.timestamp >= target_timestamp:
                self.last_timestamp = swap.timestamp
                return swap, spot
    
    def read_all_depth_by_interval(self, interval_ms=1000, k=3):
        """
        按指定的时间间隔读取所有深度数据并转为DataFrame
        
        Args:
            interval_ms (int): 时间间隔，单位毫秒，默认1000ms(1秒)
            k (int): 保留的深度层数，默认为3
            
        Returns:
            polars.DataFrame: 包含按间隔采样的深度数据
        """
        if self.data_type != "Depth":
            raise ValueError("当前数据类型不是Depth，请使用Depth类型初始化Provider")
            
        # 重置读取位置和时间戳
        self._seek_to_start_time()
        self.current_swap = self._read_next_valid_swap()
        self.last_timestamp = None
        
        data = []
        count = 0
        total_count = 0
        skipped_count = 0
        
        # 确保k不超过最大深度
        from crypto.common import MAX_DEPTH_SIZE
        k = min(k, MAX_DEPTH_SIZE)
        
        start_time = time.time()
        
        while True:
            swap, spot = self.read_next_depth_by_interval(interval_ms)
            if swap is None or spot is None:
                break
                
            total_count += 1
            
            # 每100条打印一次进度
            if total_count % 10000 == 0:
                elapsed = time.time() - start_time
                print(f"已读取 {total_count} 条间隔数据(保留: {count}, 跳过: {skipped_count})... 耗时: {elapsed:.2f}秒")
                
            # 收集数据
            timestamp = swap.timestamp
            datetime_val = datetime.fromtimestamp(timestamp)
            
            row_data = {
                "timestamp": timestamp,
                "datetime": datetime_val,
            }
            
            # 处理swap深度数据
            for i in range(k):
                # 检查是否有足够深度，如果没有则使用NaN
                bid_price = swap.bid_prices[i] if i < swap.size else np.nan
                bid_amount = swap.bid_amounts[i] if i < swap.size else np.nan
                ask_price = swap.ask_prices[i] if i < swap.size else np.nan
                ask_amount = swap.ask_amounts[i] if i < swap.size else np.nan
                
                row_data[f"swap_bid{i+1}"] = bid_price
                row_data[f"swap_bid_amount{i+1}"] = bid_amount
                row_data[f"swap_ask{i+1}"] = ask_price
                row_data[f"swap_ask_amount{i+1}"] = ask_amount
            
            # 处理spot深度数据
            for i in range(k):
                # 检查是否有足够深度，如果没有则使用NaN
                bid_price = spot.bid_prices[i] if i < spot.size else np.nan
                bid_amount = spot.bid_amounts[i] if i < spot.size else np.nan
                ask_price = spot.ask_prices[i] if i < spot.size else np.nan
                ask_amount = spot.ask_amounts[i] if i < spot.size else np.nan
                
                row_data[f"spot_bid{i+1}"] = bid_price
                row_data[f"spot_bid_amount{i+1}"] = bid_amount
                row_data[f"spot_ask{i+1}"] = ask_price
                row_data[f"spot_ask_amount{i+1}"] = ask_amount
            
            data.append(row_data)
            count += 1
            
        elapsed = time.time() - start_time
        print(f"总共读取了 {count} 条间隔数据，跳过了 {skipped_count} 条数据")
        print(f"原始数据总量约为: {total_count} 条，压缩比: {count/total_count if total_count > 0 else 0:.2%}")
        print(f"总耗时: {elapsed:.2f}秒，平均每秒处理 {total_count/elapsed if elapsed > 0 else 0:.2f} 条数据")
        
        # 创建DataFrame
        if data:
            return pl.DataFrame(data)
        else:
            # 返回空DataFrame但保持列结构
            empty_df_cols = {"timestamp": [], "datetime": []}
            
            # 添加深度列
            for i in range(k):
                empty_df_cols[f"swap_bid{i+1}"] = []
                empty_df_cols[f"swap_bid_amount{i+1}"] = []
                empty_df_cols[f"swap_ask{i+1}"] = []
                empty_df_cols[f"swap_ask_amount{i+1}"] = []
                empty_df_cols[f"spot_bid{i+1}"] = []
                empty_df_cols[f"spot_bid_amount{i+1}"] = []
                empty_df_cols[f"spot_ask{i+1}"] = []
                empty_df_cols[f"spot_ask_amount{i+1}"] = []
                
            return pl.DataFrame(empty_df_cols)
    
    def read_all_fast_data(self):
        """
        读取所有Fast数据并转换为polars DataFrame
        
        Returns:
            polars.DataFrame: 包含所有Fast数据的DataFrame
        """
        if self.data_type != "Fast":
            raise ValueError("当前数据类型不是Fast，请使用Fast类型初始化Provider")
            
        # 重置读取位置
        self._seek_to_start_time()
        self.current_swap = self._read_next_valid_swap()
        
        data = []
        count = 0
        
        while True:
            swap, spot = self.read_next()
            if swap is None or spot is None:
                break
                
            # 每10000条打印一次进度
            if count % 10000 == 0:
                print(f"已读取 {count} 条Fast数据...")
                
            # 收集数据
            timestamp = swap.timestamp
            datetime_val = datetime.fromtimestamp(timestamp)
            
            data.append({
                "timestamp": timestamp,
                "datetime": datetime_val,
                "swap_bid1": swap.bid1,
                "swap_ask1": swap.ask1,
                "spot_bid1": spot.bid1,
                "spot_ask1": spot.ask1
            })
            
            count += 1
            
        print(f"总共读取了 {count} 条Fast数据")
        
        # 创建DataFrame
        if data:
            return pl.DataFrame(data)
        else:
            # 返回空DataFrame但保持列结构
            return pl.DataFrame({
                "timestamp": [],
                "datetime": [],
                "swap_bid1": [],
                "swap_ask1": [],
                "spot_bid1": [],
                "spot_ask1": []
            })
            
    def read_all_depth_data(self, k=3):
        """
        读取所有Depth数据并转换为polars DataFrame，保留前k层深度
        
        Args:
            k (int): 保留的深度层数，默认为3
            
        Returns:
            polars.DataFrame: 包含所有Depth数据的DataFrame
        """
        if self.data_type != "Depth":
            raise ValueError("当前数据类型不是Depth，请使用Depth类型初始化Provider")
            
        # 重置读取位置
        self._seek_to_start_time()
        self.current_swap = self._read_next_valid_swap()
        
        data = []
        count = 0
        
        # 确保k不超过最大深度
        from crypto.common import MAX_DEPTH_SIZE
        k = min(k, MAX_DEPTH_SIZE)
        
        while True:
            swap, spot = self.read_next()
            if swap is None or spot is None:
                break
                
            # 每10000条打印一次进度
            if count % 10000 == 0:
                print(f"已读取 {count} 条Depth数据...")
                
            # 收集数据
            timestamp = swap.timestamp
            datetime_val = datetime.fromtimestamp(timestamp)
            
            row_data = {
                "timestamp": timestamp,
                "datetime": datetime_val,
            }
            
            # 处理swap深度数据
            for i in range(k):
                # 检查是否有足够深度，如果没有则使用NaN
                bid_price = swap.bid_prices[i] if i < swap.size else np.nan
                bid_amount = swap.bid_amounts[i] if i < swap.size else np.nan
                ask_price = swap.ask_prices[i] if i < swap.size else np.nan
                ask_amount = swap.ask_amounts[i] if i < swap.size else np.nan
                
                row_data[f"swap_bid{i+1}"] = bid_price
                row_data[f"swap_bid_amount{i+1}"] = bid_amount
                row_data[f"swap_ask{i+1}"] = ask_price
                row_data[f"swap_ask_amount{i+1}"] = ask_amount
            
            # 处理spot深度数据
            for i in range(k):
                # 检查是否有足够深度，如果没有则使用NaN
                bid_price = spot.bid_prices[i] if i < spot.size else np.nan
                bid_amount = spot.bid_amounts[i] if i < spot.size else np.nan
                ask_price = spot.ask_prices[i] if i < spot.size else np.nan
                ask_amount = spot.ask_amounts[i] if i < spot.size else np.nan
                
                row_data[f"spot_bid{i+1}"] = bid_price
                row_data[f"spot_bid_amount{i+1}"] = bid_amount
                row_data[f"spot_ask{i+1}"] = ask_price
                row_data[f"spot_ask_amount{i+1}"] = ask_amount
            
            data.append(row_data)
            count += 1
            
        print(f"总共读取了 {count} 条Depth数据")
        
        # 创建DataFrame
        if data:
            return pl.DataFrame(data)
        else:
            # 返回空DataFrame但保持列结构
            empty_df_cols = {"timestamp": [], "datetime": []}
            
            # 添加深度列
            for i in range(k):
                empty_df_cols[f"swap_bid{i+1}"] = []
                empty_df_cols[f"swap_bid_amount{i+1}"] = []
                empty_df_cols[f"swap_ask{i+1}"] = []
                empty_df_cols[f"swap_ask_amount{i+1}"] = []
                empty_df_cols[f"spot_bid{i+1}"] = []
                empty_df_cols[f"spot_bid_amount{i+1}"] = []
                empty_df_cols[f"spot_ask{i+1}"] = []
                empty_df_cols[f"spot_ask_amount{i+1}"] = []
                
            return pl.DataFrame(empty_df_cols)
        
    def close(self):
        for reader in self.swap_readers:
            reader.close()
        for reader in self.spot_readers:
            reader.close()


if __name__ == "__main__":
    # 测试用的instruments配置
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
    
    date = "2025-01-06"
    hour_offset = 2  # 从凌晨2点开始
    
    # 测试按间隔读取Depth数据
    try:
        print("\n=== 测试按间隔读取Depth数据 ===")
        provider = MarketDataProvider(instruments, date, hour_offset, data_type="Depth")
        
        print(f"开始时间: {provider.start_datetime}")
        print(f"结束时间: {provider.end_datetime}")
        
        # 测试read_next_depth_by_interval函数
        print("\n测试read_next_depth_by_interval函数 (interval_ms=1000):")
        provider.last_timestamp = None  # 重置时间戳
        
        # 读取前10条按秒间隔的数据
        for i in range(10):
            swap, spot = provider.read_next_depth_by_interval(interval_ms=1000)
            if swap is None or spot is None:
                print("数据读取完毕")
                break
                
            print(f"数据 #{i}: Swap时间戳: {swap.timestamp} ({datetime.fromtimestamp(swap.timestamp)})")
            
        # 测试read_all_depth_by_interval函数
        print("\n测试read_all_depth_by_interval函数 (interval_ms=1000, k=2):")
        df = provider.read_all_depth_by_interval(interval_ms=1000, k=2)
        
        # 显示DataFrame信息
        print(f"DataFrame形状: {df.shape}")
        print("DataFrame列名:")
        print(df.columns)
        if not df.is_empty():
            print("\nDataFrame前3行:")
            print(df.head(3))
            
            # 计算时间间隔统计
            if len(df) > 1:
                df = df.with_columns(
                    pl.col("timestamp").diff().alias("time_diff")
                )
                print("\n时间间隔统计 (秒):")
                print(f"平均间隔: {df['time_diff'].mean():.3f}秒")
                print(f"最小间隔: {df['time_diff'].min():.3f}秒")
                print(f"最大间隔: {df['time_diff'].max():.3f}秒")
                
        provider.close()
        
    except Exception as e:
        print(f"按间隔读取测试过程中出错: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if 'provider' in locals():
            provider.close()
            print("数据提供者已关闭")
