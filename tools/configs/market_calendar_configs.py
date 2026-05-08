"""
市场交易日历和特殊休市处理配置。

维护影响：
- `MARKET_CALENDAR_NAMES` 决定 US/CN/HK/KR 使用哪个 pandas-market-calendars 日历。
- `MARKET_CLOSE_BUFFER_HOURS` 决定收盘后等待多久才认为完整日线可确认。
- 韩国节假日置零配置只处理明确已知的“目标估值日休市但旧行情可能被重复计入”的场景。
"""

MARKET_CALENDAR_NAMES = {
    "US": "NYSE",
    "CN": "SSE",
    "HK": "HKEX",
    "KR": "XKRX",
}


MARKET_CLOSE_BUFFER_HOURS = 2


KR_MARKET_ZERO_HOLIDAY_MD = {
    "05-05": "韩国儿童节",
}


KR_MARKET_ZERO_HOLIDAYS = {
    "2026-05-05": "韩国儿童节",
}


__all__ = [
    "MARKET_CALENDAR_NAMES",
    "MARKET_CLOSE_BUFFER_HOURS",
    "KR_MARKET_ZERO_HOLIDAY_MD",
    "KR_MARKET_ZERO_HOLIDAYS",
]
