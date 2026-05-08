"""
ETF/FOF/指数联接基金的代理资产配置入口。

真实配置已迁移到 `tools.configs.fund_proxy_configs`。本文件保留原导入入口，
方便旧代码继续使用 `from tools.fund_proxy_map import DEFAULT_FUND_PROXY_MAP`。
"""

from tools.configs.fund_proxy_configs import DEFAULT_FUND_PROXY_MAP, OVERSEAS_VALID_HOLDING_BOOST


__all__ = ["DEFAULT_FUND_PROXY_MAP", "OVERSEAS_VALID_HOLDING_BOOST"]
