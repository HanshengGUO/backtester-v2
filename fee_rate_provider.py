from datetime import datetime, timedelta
import os
import json
import pytz
import logging

class FeeRateProvider:
    """
    资金费率提供器，用于获取不同交易所的资金费率数据
    支持缓存到文件并优先从缓存读取
    """
    
    def __init__(self, exchange="okex", cache_dir="cache/funding_rates"):
        """
        初始化资金费率提供器
        
        Args:
            exchange (str): 交易所名称，目前支持 "okex"，后续可扩展
            cache_dir (str): 缓存目录，默认为 "cache/funding_rates"
        """
        self.exchange = exchange.lower()
        self.cache_dir = cache_dir
        self.api = self._init_api()
        self.cache = {}
        
        # 确保缓存目录存在
        os.makedirs(os.path.join(self.cache_dir, self.exchange), exist_ok=True)
        
        # 设置时区
        self.tz = pytz.timezone('Asia/Shanghai')

    def _init_api(self):
        """
        初始化对应交易所的API
        
        Returns:
            API实例
        """
        if self.exchange == "okex":
            from crypto.api.okex.api_v5 import OkexAPI
            return OkexAPI()
        else:
            raise ValueError(f"不支持的交易所: {self.exchange}")
    
    def _get_cache_path(self, date_str):
        """
        获取缓存文件路径
        
        Args:
            date_str (str): 日期字符串，格式为 YYYY-MM-DD
            
        Returns:
            str: 缓存文件路径
        """
        return os.path.join(self.cache_dir, self.exchange, f"{date_str}.json")
    
    def _load_from_cache(self, date_str):
        """
        从缓存加载资金费率数据
        
        Args:
            date_str (str): 日期字符串，格式为 YYYY-MM-DD
            
        Returns:
            dict: 资金费率数据，如果缓存不存在则返回None
        """
        cache_path = self._get_cache_path(date_str)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.warning(f"读取缓存文件失败: {cache_path}, 错误: {e}")
        return None
    
    def _save_to_cache(self, date_str, data):
        """
        保存资金费率数据到缓存
        
        Args:
            date_str (str): 日期字符串，格式为 YYYY-MM-DD
            data (dict): 资金费率数据
        """
        cache_path = self._get_cache_path(date_str)
        try:
            with open(cache_path, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logging.warning(f"保存缓存文件失败: {cache_path}, 错误: {e}")
    
    def _fetch_funding_rates(self, start_time, end_time, inst_id="BTC-USDT-SWAP"):
        """
        直接从API获取指定时间范围内的资金费率
        
        Args:
            start_time (datetime): 开始时间（左开）
            end_time (datetime): 结束时间（右闭）
            inst_id (str): 交易对ID，默认为 "BTC-USDT-SWAP"
            
        Returns:
            dict: 资金费率数据，key为时间戳(秒)，value为费率
        """
        # 转换为毫秒级时间戳字符串
        start_timestamp = str(int(start_time.timestamp() * 1000))
        end_timestamp = str(int(end_time.timestamp() * 1000))

        print("start_timestamp, end_timestamp to query the api", start_timestamp, end_timestamp)
        
        if self.exchange == "okex":
            json_data = self.api.get_funding_rate_history(
                instId=inst_id, 
                after=end_timestamp,  # OKEX API中，after参数是结束时间
                before=start_timestamp  # before参数是开始时间
            )
            
            result = {}
            for item in json_data['data']:
                timestamp = int(item['fundingTime']) / 1000  # 转换为秒级时间戳
                fee_rate = float(item['fundingRate'])
                result[int(timestamp)] = fee_rate
            return result
        else:
            raise ValueError(f"不支持的交易所: {self.exchange}")
    
    def get_daily_funding_rates(self, date_str, inst_id="BTC-USDT-SWAP"):
        """
        获取指定日期的资金费率
        左开右闭区间，根据交易所规则获取当日对应时间点的资金费
        
        Args:
            date_str (str): 日期字符串，格式为 YYYY-MM-DD
            inst_id (str): 交易对ID，默认为 "BTC-USDT-SWAP"
            
        Returns:
            dict: 资金费率数据，key为时间戳(秒)，value为费率
        """
        # 先尝试从缓存加载
        result = self._load_from_cache(date_str)
        
        if result is None:
            # 缓存不存在，从API获取
            date = datetime.strptime(date_str, '%Y-%m-%d')
            
            # 设置查询时间范围，获取当天和次日凌晨的数据
            start_time = self.tz.localize(datetime(date.year, date.month, date.day, 0, 0, 0))
            end_time = self.tz.localize(datetime(date.year, date.month, date.day, 23, 59, 59))
            next_day_start = start_time + timedelta(days=1)
            
            # 获取当天到次日的所有资金费
            result = self._fetch_funding_rates(start_time, end_time, inst_id)
            
            # 特别处理第二天的0点数据
            next_day_result = self._fetch_funding_rates(
                end_time, 
                self.tz.localize(datetime(next_day_start.year, next_day_start.month, next_day_start.day, 0, 0, 1)),
                inst_id
            )
            
            # 合并结果
            result.update(next_day_result)
            
            # 把int类型的key转为字符串，以便JSON序列化
            result = {str(k): v for k, v in result.items()}
            
            # 保存到缓存
            self._save_to_cache(date_str, result)
        
        # 转换回来，所有key变回int类型
        result = {int(k): v for k, v in result.items()}
        
        return result
    
    def get_next_funding_rate(self, timestamp, inst_id="BTC-USDT-SWAP"):
        """
        获取指定时间戳之后的下一个资金费率
        
        Args:
            timestamp (int): 时间戳，支持秒级或毫秒级
            inst_id (str): 交易对ID，默认为 "BTC-USDT-SWAP"
            
        Returns:
            tuple: (时间戳, 费率)，如果没有找到则返回(None, None)
        """
        # 将毫秒级时间戳转换为秒级
        if timestamp > 1e12:
            timestamp = int(timestamp / 1000)
        
        # 转换为datetime对象
        dt = datetime.fromtimestamp(timestamp, self.tz)
        
        # 获取当天的日期字符串
        date_str = dt.strftime('%Y-%m-%d')
        
        # 获取当天的所有资金费率
        funding_rates = self.get_daily_funding_rates(date_str, inst_id)
        
        # 找出大于当前时间戳的最小时间戳
        next_ts = None
        next_rate = None
        
        for ts, rate in funding_rates.items():
            if ts > timestamp and (next_ts is None or ts < next_ts):
                next_ts = ts
                next_rate = rate
        
        # 如果当天没有找到，尝试查询下一天
        if next_ts is None:
            next_date = dt + timedelta(days=1)
            next_date_str = next_date.strftime('%Y-%m-%d')
            next_funding_rates = self.get_daily_funding_rates(next_date_str, inst_id)
            
            for ts, rate in next_funding_rates.items():
                if next_ts is None or ts < next_ts:
                    next_ts = ts
                    next_rate = rate
        
        return (next_ts, next_rate) if next_ts is not None else (None, None)
    
    def get_prev_funding_rate(self, timestamp, inst_id="BTC-USDT-SWAP"):
        """
        获取指定时间戳之前的上一个资金费率
        
        Args:
            timestamp (int): 时间戳，支持秒级或毫秒级
            inst_id (str): 交易对ID，默认为 "BTC-USDT-SWAP"
            
        Returns:
            tuple: (时间戳, 费率)，如果没有找到则返回(None, None)
        """
        # 将毫秒级时间戳转换为秒级
        if timestamp > 1e12:
            timestamp = int(timestamp / 1000)
        
        # 转换为datetime对象
        dt = datetime.fromtimestamp(timestamp, self.tz)
        
        # 获取当天的日期字符串
        date_str = dt.strftime('%Y-%m-%d')
        
        # 获取当天的所有资金费率
        funding_rates = self.get_daily_funding_rates(date_str, inst_id)
        
        # 找出小于当前时间戳的最大时间戳
        prev_ts = None
        prev_rate = None
        
        for ts, rate in funding_rates.items():
            if ts < timestamp and (prev_ts is None or ts > prev_ts):
                prev_ts = ts
                prev_rate = rate
        
        # 如果当天没有找到，尝试查询前一天
        if prev_ts is None:
            prev_date = dt - timedelta(days=1)
            prev_date_str = prev_date.strftime('%Y-%m-%d')
            prev_funding_rates = self.get_daily_funding_rates(prev_date_str, inst_id)
            
            for ts, rate in prev_funding_rates.items():
                if prev_ts is None or ts > prev_ts:
                    prev_ts = ts
                    prev_rate = rate
        
        return (prev_ts, prev_rate) if prev_ts is not None else (None, None)


if __name__ == "__main__":
    # 测试代码
    provider = FeeRateProvider("okex")
    
    for i in range(1, 100):
        # 测试获取当天的资金费率
        print("====== 测试获取当天的资金费率 ======")
        rates = provider.get_daily_funding_rates("2025-03-15")
        for ts, rate in rates.items():
            dt = datetime.fromtimestamp(ts)
            print(f"{dt}: {rate}")
        
        # 测试获取下一个资金费率
        print("\n====== 测试获取下一个资金费率 ======")
        timestamp = int(datetime(2025, 3, 15, 10, 0, 0).timestamp())
        next_ts, next_rate = provider.get_next_funding_rate(timestamp)
        if next_ts:
            dt = datetime.fromtimestamp(next_ts)
            print(f"下一个资金费率时间: {dt}, 费率: {next_rate}")
        else:
            print("未找到下一个资金费率")
        
        # 测试获取上一个资金费率
        print("\n====== 测试获取上一个资金费率 ======")
        timestamp = int(datetime(2025, 3, 15, 10, 0, 0).timestamp())
        prev_ts, prev_rate = provider.get_prev_funding_rate(timestamp)
        if prev_ts:
            dt = datetime.fromtimestamp(prev_ts)
            print(f"上一个资金费率时间: {dt}, 费率: {prev_rate}")
        else:
            print("未找到上一个资金费率")
