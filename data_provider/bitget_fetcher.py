# -*- coding: utf-8 -*-
"""
===================================
BitgetFetcher - BitGet 合约数据源
===================================

BitGet 合约 API 获取 K线和实时行情

API 文档:
- 实时行情: https://www.bitget.com/zh-CN/api-doc/contract/market/Get-Ticker
- K线数据: https://www.bitget.com/zh-CN/api-doc/contract/market/Get-Candle-Data
- 历史K线: https://www.bitget.com/zh-CN/api-doc/contract/market/Get-History-Candle-Data

特点：
- 无需 API Key（公开市场数据）
- 使用 USDT 本位合约 (usdt-futures)
- 可作为 BinanceFetcher 的备份数据源
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

# BitGet API 基础地址
BITGET_API_BASE = "https://api.bitget.com"

# 产品类型: USDT 本位合约
PRODUCT_TYPE = "usdt-futures"

# 请求超时
_REQUEST_TIMEOUT = 10

# 重试策略
_TRANSIENT_EXCEPTIONS = (
    requests.exceptions.SSLError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def is_bitget_stock_token(code: str) -> bool:
    """
    判断是否为 BitGet 股票代币（如 SPCXUSDT、AAPLUSDT）
    
    规则：以 USDT 结尾
    """
    code = (code or "").strip().upper()
    if not code.endswith("USDT") or code == "USDT":
        return False
    return True


def extract_bitget_symbol(code: str) -> str:
    """从股票代码提取 BitGet 交易对（如 spcxusdt -> SPCXUSDT）"""
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


class BitgetFetcher(BaseFetcher):
    """
    BitGet 合约数据源
    
    支持：
    - 日线数据 (daily_data)
    - 实时行情 (realtime_quote)
    
    使用 BitGet 合约市场 API (v2)，无需 API Key
    """
    
    name = "BitgetFetcher"
    priority = int(os.getenv("BITGET_PRIORITY", "0"))
    
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
        end_date: str,
        interval: str = "1d",
    ) -> pd.DataFrame:
        """
        获取 K 线数据

        API: GET /api/v2/mix/market/history-candles
        文档: https://www.bitget.com/zh-CN/api-doc/contract/market/Get-History-Candle-Data
        """
        symbol = extract_bitget_symbol(stock_code)

        # 转换日期为毫秒时间戳
        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)

        # BitGet 使用大写格式：1m/5m/15m/30m/1h/4h/1d/1w
        # 将 "1d" 转换为 "1D"，"1h" 保持不变
        bitget_interval = interval.upper() if interval.lower() == "1d" else interval

        # 使用合约历史K线接口 (v2)
        url = f"{BITGET_API_BASE}/api/v2/mix/market/history-candles"
        params = {
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
            "granularity": bitget_interval,
            "startTime": str(start_ts),
            "endTime": str(end_ts),
            "limit": "200",  # 最大 200 条
        }
        
        try:
            resp = _get_with_retry(url, params)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("code") != "00000" or not data.get("data"):
                logger.warning(f"[BitgetFetcher] API 返回错误: {data.get('msg', 'Unknown error')}")
                raise DataFetchError(f"BitgetFetcher API 错误: {data.get('msg', 'Unknown error')}")
            
            candles = data["data"]
            if not candles:
                logger.warning(f"[BitgetFetcher] K线数据为空: {symbol}")
                raise DataFetchError(f"BitgetFetcher K线数据为空: {symbol}")
            
            logger.debug(f"[BitgetFetcher] 获取K线成功: {symbol}, {len(candles)} 条")
            
            # 转换数据格式
            # BitGet 合约返回格式: [timestamp, open, high, low, close, base_volume, quote_volume]
            rows = []
            for candle in candles:
                ts = int(candle[0])
                dt = datetime.fromtimestamp(ts / 1000)
                # 小时级别保留时间，日期级别只保留日期
                if "m" in interval or "h" in interval:
                    date_str = dt.strftime("%Y-%m-%d %H:%M")
                else:
                    date_str = dt.strftime("%Y-%m-%d")
                rows.append({
                    "date": date_str,
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5]),
                    "amount": float(candle[6]) if len(candle) > 6 else 0.0,
                })
            
            df = pd.DataFrame(rows)
            
            # 过滤日期范围
            df["date"] = pd.to_datetime(df["date"])
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]
            
            return df
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 451:
                logger.error(f"[BitgetFetcher] HTTP错误: 451 (地区不可用)")
                raise DataFetchError(f"BitgetFetcher HTTP 451: 地区不可用")
            logger.error(f"[BitgetFetcher] HTTP错误: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"[BitgetFetcher] 请求错误: {e}")
            raise DataFetchError(f"BitgetFetcher 请求错误: {e}")
        except Exception as e:
            logger.error(f"[BitgetFetcher] 获取 K 线失败: {symbol}, {e}")
            raise DataFetchError(f"BitgetFetcher 获取 K 线失败: {symbol}, {e}")
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """标准化数据列名"""
        if df.empty:
            return df
        
        # 直接返回，因为 _fetch_raw_data 已经返回标准化格式
        return df
    
    def get_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """
        获取实时行情
        
        API: GET /api/v2/mix/market/ticker
        文档: https://www.bitget.com/zh-CN/api-doc/contract/market/Get-Ticker
        
        Args:
            stock_code: 股票代码（如 SPCXUSDT）
            
        Returns:
            UnifiedRealtimeQuote 或 None
        """
        symbol = extract_bitget_symbol(stock_code)
        
        # 使用合约实时行情接口 (v2)
        url = f"{BITGET_API_BASE}/api/v2/mix/market/ticker"
        params = {
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
        }
        
        try:
            resp = _get_with_retry(url, params)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("code") != "00000" or not data.get("data"):
                logger.warning(f"[BitgetFetcher] 实时行情 API 返回错误: {data.get('msg', 'Unknown error')}")
                return None
            
            ticker_list = data["data"]
            if not ticker_list:
                logger.warning(f"[BitgetFetcher] 实时行情返回空数据: {symbol}")
                return None
            
            ticker = ticker_list[0]
            
            # change24h 是涨跌幅百分比（如 -0.05638 表示 -5.638%）
            change_pct = float(ticker.get("change24h", 0))
            # 转换为涨跌额：price * change_pct / 100
            price = float(ticker.get("lastPr", 0))
            change = price * change_pct / 100 if price and change_pct else 0.0
            
            return UnifiedRealtimeQuote(
                stock_code=symbol,
                price=price,
                change=change,
                change_pct=change_pct,
                volume=float(ticker.get("baseVolume", 0)),
                turnover=float(ticker.get("quoteVolume", 0)),
                high=float(ticker.get("high24h", 0)),
                low=float(ticker.get("low24h", 0)),
                source=RealtimeSource.BITGET,
            )
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 451:
                logger.error(f"[BitgetFetcher] 实时行情 HTTP 451")
                return None
            logger.warning(f"[BitgetFetcher] 实时行情 HTTP错误: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"[BitgetFetcher] 实时行情请求错误: {e}")
            return None
        except Exception as e:
            logger.warning(f"[BitgetFetcher] 实时行情获取失败: {e}")
            return None
    
    def health_check(self) -> bool:
        """健康检查 - 使用合约接口"""
        url = f"{BITGET_API_BASE}/api/v2/mix/market/ticker"
        params = {
            "symbol": "BTCUSDT",
            "productType": PRODUCT_TYPE,
        }
        
        try:
            resp = _get_with_retry(url, params)
            resp.raise_for_status()
            data = resp.json()
            return data.get("code") == "00000"
        except Exception as e:
            logger.warning(f"[BitgetFetcher] 健康检查失败: {e}")
            return False
