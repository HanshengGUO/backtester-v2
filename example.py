import os
import logging
from datetime import datetime

from trading_signals import TradingSignalFirst
from trading_strategies import TradingStratsFirst
from backtester import Backtester

def main():
    """
    主函数，演示如何使用交易策略和回测系统
    """
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger("Backtester")

    # 设置数据路径和交易对
    instruments = {
        "swap": {
            "name": "binance_swap_btcusdt",
            "data_path": os.path.join(os.environ.get("DATA_ROOT", "/data"), "binance/btcusdt_swap"),
        },
        "spot": {
            "name": "binance_spot_btcusdt",
            "data_path": os.path.join(os.environ.get("DATA_ROOT", "/data"), "binance/btcusdt_spot"),
        }
    }
    
    # 设置回测日期
    dates = [
        "2025-02-01", "2025-02-02", "2025-02-03", "2025-02-04", "2025-02-05",
        "2025-02-06", "2025-02-07", "2025-02-08", "2025-02-09", "2025-02-10"
    ]
    
    # 创建交易策略
    strategy = TradingStratsFirst(
        capital=10000,  # 10000 USDT
        max_positions=2,
        fee_rate=0.00015,  # Taker费率 0.015%
        funding_fee_enabled=True,
        inst_id="BTC-USDT-SWAP",
        exchange="binance"
    )
    
    # 创建回测系统
    backtester = Backtester(
        strategy=strategy,
        instruments=instruments,
        dates=dates,
        logger=logger,
        window_size=10,
        interval_ms=1000,
        data_type="Depth"
    )
    
    # 运行回测
    logger.info("开始回测...")
    results = backtester.run(debug_mode=False, parallel=True)
    
    # 输出回测结果
    logger.info("\n=== 回测结果摘要 ===")
    logger.info(f"初始资金: {results['initial_capital']:.2f} USDT")
    logger.info(f"最终资金: {results['final_capital']:.2f} USDT")
    logger.info(f"净盈亏: {results['net_pnl']:.2f} USDT")
    logger.info(f"年化收益率: {results['annual_return']:.2f}%")
    logger.info(f"胜率: {results['win_rate']:.2f}")
    logger.info(f"交易次数: {results['trade_count']}")
    logger.info(f"总手续费: {results['total_fee']:.2f} USDT")
    logger.info(f"总资金费: {results['total_funding_fee']:.2f} USDT")
    
    # 绘制结果
    backtester.plot_results(save_path=f"backtest_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
    
    return results

def parameter_sweep():
    """
    参数扫描，尝试不同的参数组合
    """
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"parameter_sweep_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger("ParameterSweep")

    # 设置数据路径和交易对
    instruments = {
        "swap": {
            "name": "binance_swap_btcusdt",
            "data_path": os.path.join(os.environ.get("DATA_ROOT", "/data"), "binance/btcusdt_swap"),
        },
        "spot": {
            "name": "binance_spot_btcusdt",
            "data_path": os.path.join(os.environ.get("DATA_ROOT", "/data"), "binance/btcusdt_spot"),
        }
    }
    
    # 设置回测日期
    dates = [
        "2025-02-01", "2025-02-02", "2025-02-03", "2025-02-04", "2025-02-05",
        "2025-02-06", "2025-02-07", "2025-02-08", "2025-02-09", "2025-02-10"
    ]
    
    # 参数组合
    param_combinations = []
    
    # 不同的资金费启用设置
    for funding_fee_enabled in [True, False]:
        # 不同的最大持仓数
        for max_positions in [1, 2, 3]:
            # 创建交易策略
            strategy = TradingStratsFirst(
                capital=10000,  # 10000 USDT
                max_positions=max_positions,
                fee_rate=0.00015,  # Taker费率 0.015%
                funding_fee_enabled=funding_fee_enabled,
                inst_id="BTC-USDT-SWAP",
                exchange="binance"
            )
            
            param_combinations.append({
                "strategy": strategy,
                "max_positions": max_positions,
                "funding_fee_enabled": funding_fee_enabled
            })
    
    # 运行所有参数组合
    results = []
    for i, params in enumerate(param_combinations):
        logger.info(f"\n=== 参数组合 {i+1}/{len(param_combinations)} ===")
        logger.info(f"最大持仓数: {params['max_positions']}")
        logger.info(f"资金费启用: {params['funding_fee_enabled']}")
        
        # 创建回测系统
        backtester = Backtester(
            strategy=params["strategy"],
            instruments=instruments,
            dates=dates,
            logger=logger,
            window_size=10,
            interval_ms=1000,
            data_type="Depth"
        )
        
        # 运行回测
        result = backtester.run(debug_mode=False, parallel=True)
        
        # 记录结果
        results.append({
            "params": params,
            "result": result
        })
        
        # 输出回测结果
        logger.info("\n=== 回测结果摘要 ===")
        logger.info(f"净盈亏: {result['net_pnl']:.2f} USDT")
        logger.info(f"年化收益率: {result['annual_return']:.2f}%")
        logger.info(f"胜率: {result['win_rate']:.2f}")
        logger.info(f"交易次数: {result['trade_count']}")
    
    # 找出最佳参数组合
    best_result = max(results, key=lambda x: x["result"]["annual_return"])
    
    logger.info("\n=== 最佳参数组合 ===")
    logger.info(f"最大持仓数: {best_result['params']['max_positions']}")
    logger.info(f"资金费启用: {best_result['params']['funding_fee_enabled']}")
    logger.info(f"净盈亏: {best_result['result']['net_pnl']:.2f} USDT")
    logger.info(f"年化收益率: {best_result['result']['annual_return']:.2f}%")
    logger.info(f"胜率: {best_result['result']['win_rate']:.2f}")
    logger.info(f"交易次数: {best_result['result']['trade_count']}")
    
    return results, best_result

if __name__ == "__main__":
    # 运行主函数
    main()
    
    # 或者运行参数扫描
    # results, best_result = parameter_sweep() 