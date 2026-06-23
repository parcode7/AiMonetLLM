# -*- coding: utf-8 -*-
"""
===================================
BinanceFetcher - 币安股票代币数据源
===================================

币安支持股票代币化交易（如 SPCXUSDT、AAPLUSDT、TSLAUSDT 等）
通过币安公开 API 获取 K线和实时行情

API 文档: https://developers.binance.com/docs/simple_earn/history/get-kline

特点：
- 无需 API Key（公开市场数据）
- 24/7 可交易
- 数据延迟低
"""

import logging
import os
import time
from datetime import datetime
from typing import Optional, List, Dict, Any

import pandas as pd
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS
from .realtime_types import UnifiedRealtimeQuote, RealtimeSource

logger = logging.getLogger(__name__)

# 币安 API 基础地址
BINANCE_API_BASE = "https://api.binance.com"

# 股票代币后缀
STOCK_TOKEN_SUFFIX = "USDT"

# 请求超时
_REQUEST_TIMEOUT = 10

# 重试策略
_TRANSIENT_EXCEPTIONS = (
    requests.exceptions.SSLError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def is_binance_stock_token(code: str) -> bool:
    """
    判断是否为币安股票代币（如 SPCXUSDT、AAPLUSDT）
    
    规则：以 USDT 结尾，且不是纯加密货币（如 BTCUSDT）
    币安股票代币的特点是有对应的美股标的
    """
    code = (code or "").strip().upper()
    if not code.endswith(STOCK_TOKEN_SUFFIX) or code == STOCK_TOKEN_SUFFIX:
        return False
    
    # 排除常见的纯加密货币（可选）
    # 这里不做排除，因为用户可能确实想分析 BTCUSDT
    # 如果需要区分，可以维护一个股票代币列表
    
    return True


def extract_binance_symbol(code: str) -> str:
    """从股票代码提取币安交易对（如 spcxusdt -> SPCXUSDT）"""
    return code.strip().upper()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(_TRANSIENT_EXCEPTIONS),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _get_with_retry(url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
    """GET with retry on transient network errors."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    return requests.get(url, headers=headers, params=params or {}, timeout=_REQUEST_TIMEOUT)


class BinanceFetcher(BaseFetcher):
    """
    币安数据源 - 股票代币
    
    支持：
    - 股票代币化交易对（SPCXUSDT、AAPLUSDT、TSLAUSDT 等）
    - K线数据（日线/周线等）
    - 24小时行情
    
    使用示例：
        fetcher = BinanceFetcher()
        quote = fetcher.get_realtime_quote("SPCXUSDT")
        df = fetcher.get_daily_data("SPCXUSDT", "2024-01-01", "2024-12-31")
    """
    
    name = "BinanceFetcher"
    priority = int(os.getenv("BINANCE_PRIORITY", "0"))  # 默认最高优先级
    
    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        })
    
    def _fetch_raw_data(
        self, 
        stock_code: str, 
        start_date: str, 
        end_date: str
    ) -> pd.DataFrame:
        """
        获取 K 线数据
        
        API: GET /api/v3/klines
        文档: https://developers.binance.com/docs/simple_earn/history/get-kline
        """
        symbol = extract_binance_symbol(stock_code)
        
        # 转换日期为毫秒时间戳
        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)
        
        url = f"{BINANCE_API_BASE}/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": "1d",
            "startTime": start_ts,
            "endTime": end_ts,
            "limit": 500,
        }
        
        try:
            resp = _get_with_retry(url, params)
            resp.raise_for_status()
            data = resp.json()
            
            if not data:
                logger.warning(f"[BinanceFetcher] K线数据为空: {symbol}")
                return pd.DataFrame()
            
            # 币安 K 线字段顺序
            # [open_time, open, high, low, close, volume, close_time, quote_volume, trades, ...]
            df = pd.DataFrame(data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trades", "taker_buy_base",
                "taker_buy_quote", "ignore"
            ])
            
            logger.debug(f"[BinanceFetcher] 获取K线成功: {symbol}, {len(df)} 条")
            return df
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                # Invalid symbol 等错误
                logger.warning(f"[BinanceFetcher] 无效的交易对: {symbol}")
            else:
                logger.error(f"[BinanceFetcher] HTTP错误: {e}")
            raise DataFetchError(f"币安 K 线获取失败: {e}") from e
        except Exception as e:
            logger.error(f"[BinanceFetcher] 获取 K 线失败: {symbol}, {e}")
            raise DataFetchError(f"币安 K 线获取失败: {e}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """标准化数据列名"""
        if df.empty:
            return df
        
        result = pd.DataFrame()
        result["date"] = pd.to_datetime(df["open_time"], unit="ms").dt.tz_localize(None)
        result["open"] = pd.to_numeric(df["open"], errors="coerce")
        result["high"] = pd.to_numeric(df["high"], errors="coerce")
        result["low"] = pd.to_numeric(df["low"], errors="coerce")
        result["close"] = pd.to_numeric(df["close"], errors="coerce")
        result["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        result["amount"] = pd.to_numeric(df["quote_volume"], errors="coerce")
        
        # 计算涨跌幅
        result["pct_chg"] = result["close"].pct_change() * 100
        
        return result[STANDARD_COLUMNS]
    
    def get_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """
        获取实时行情
        
        API: GET /api/v3/ticker/24hr
        文档: https://developers.binance.com/docs/simple_earn/history/get-24hr-ticker
        """
        symbol = extract_binance_symbol(stock_code)
        
        url = f"{BINANCE_API_BASE}/api/v3/ticker/24hr"
        params = {"symbol": symbol}
        
        try:
            resp = _get_with_retry(url, params)
            resp.raise_for_status()
            data = resp.json()
            
            # 检查是否有错误响应
            if "code" in data:
                logger.warning(f"[BinanceFetcher] API错误: {data.get('msg', 'Unknown error')}")
                return None
            
            # 获取服务器时间
            time_resp = requests.get(f"{BINANCE_API_BASE}/api/v3/time", timeout=5)
            server_time = time_resp.json().get("serverTime", 0)
            
            return UnifiedRealtimeQuote(
                code=stock_code,
                name=stock_code,  # 币安不提供股票名称，使用代码作为名称
                current=float(data["lastPrice"]),
                open=float(data["openPrice"]),
                high=float(data["highPrice"]),
                low=float(data["lowPrice"]),
                close=float(data["lastPrice"]),
                prev_close=float(data["prevClosePrice"]),
                volume=float(data["volume"]),
                amount=float(data["quoteVolume"]),
                pct_chg=float(data["priceChangePercent"]),
                source=RealtimeSource.BINANCE,
                provider_timestamp=datetime.fromtimestamp(server_time / 1000).isoformat(),
                timestamp=datetime.now().isoformat(),
            )
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                error_data = e.response.json() if e.response.content else {}
                msg = error_data.get("msg", "Unknown")
                logger.warning(f"[BinanceFetcher] 无效交易对 {symbol}: {msg}")
            else:
                logger.error(f"[BinanceFetcher] HTTP错误: {e}")
            return None
        except Exception as e:
            logger.error(f"[BinanceFetcher] 获取实时行情失败: {symbol}, {e}")
            return None
    
    def is_available(self) -> bool:
        """检查数据源可用性（健康检查）"""
        try:
            url = f"{BINANCE_API_BASE}/api/v3/ping"
            resp = requests.get(url, timeout=5)
            return resp.status_code == 200
        except Exception as e:
            logger.warning(f"[BinanceFetcher] 健康检查失败: {e}")
            return False
    
    def get_supported_symbols(self) -> List[str]:
        """
        获取支持的交易对列表（股票代币）
        
        这是一个静态列表，基于币安公开的股票代币
        实际支持情况以币安 API 返回为准
        """
        # 部分已知的股票代币列表
        # 完整列表请参考: https://www.binance.com/en/support/announcement/binance-launches-b-stock
        known_stock_tokens = [
            "AAPLUSDT",   # Apple
            "TSLAUSDT",   # Tesla
            "AMZNUSDT",   # Amazon
            "MSFTUSDT",   # Microsoft
            "GOOGLUSDT",  # Google
            "NVDAUSDT",   # NVIDIA
            "METAUSDT",   # Meta (Facebook)
            "NFLXUSDT",   # Netflix
            "AMDUSDT",    # AMD
            "INTCUSDT",   # Intel
            "COINUSDT",   # Coinbase
            "SPCXUSDT",   # SP 500 Composite
            # 可继续添加...
        ]
        return known_stock_tokens
