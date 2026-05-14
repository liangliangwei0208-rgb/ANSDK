"""
手动提前刷新基金限购缓存。

默认自动刷新策略仍由 FUND_PURCHASE_LIMIT_CACHE_DAYS 控制，本脚本只是显式跳过
新鲜度判断，成功刷新后把该基金的下一次自动刷新时间顺延到本次刷新后 7 天。
"""

from __future__ import annotations

import argparse

from tools.configs.fund_universe_configs import HAIWAI_FUND_CODES
from tools.get_top10_holdings import (
    FUND_PURCHASE_LIMIT_CACHE_DAYS,
    get_fund_purchase_limit,
    print_purchase_limit_cache_refresh_summary,
)


def _normalize_fund_codes(values: list[str] | None) -> list[str]:
    if not values:
        values = list(HAIWAI_FUND_CODES)

    normalized = []
    seen = set()
    for value in values:
        code = str(value).strip()
        if not code:
            continue
        code = code.zfill(6)
        if code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="手动提前刷新基金限购缓存")
    parser.add_argument(
        "--fund-code",
        nargs="+",
        help="只刷新指定基金代码；不传则默认刷新海外基金池",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=8,
        help="单只基金限购页面请求超时秒数，默认 8 秒",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fund_codes = _normalize_fund_codes(args.fund_code)
    total = len(fund_codes)

    print(
        f"开始手动刷新基金限购缓存: fund_count={total}, "
        f"auto_refresh_days={FUND_PURCHASE_LIMIT_CACHE_DAYS}",
        flush=True,
    )

    for index, fund_code in enumerate(fund_codes, start=1):
        value = get_fund_purchase_limit(
            fund_code=fund_code,
            timeout=args.timeout,
            cache_days=FUND_PURCHASE_LIMIT_CACHE_DAYS,
            cache_enabled=True,
            force_refresh=True,
        )
        print(f"[{index}/{total}] {fund_code} -> {value}", flush=True)

    print_purchase_limit_cache_refresh_summary(cache_days=FUND_PURCHASE_LIMIT_CACHE_DAYS)
    print("基金限购缓存手动刷新完成", flush=True)


if __name__ == "__main__":
    main()
