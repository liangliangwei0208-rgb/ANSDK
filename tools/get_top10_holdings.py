"""
get_top10_holdings.py

用途
====
输入一个或多个基金代码，估算今日涨跌幅，并生成汇总表格图片。

支持类型
========
1. 普通股票型 / QDII 股票型基金
   - 从 ak.fund_portfolio_hold_em() 获取最近披露前 N 大股票持仓；
   - 默认将前 N 大持仓权重归一化到 100%；
   - 海外表可启用“有效持仓增强 + 失败持仓/未披露仓位纳斯达克100补偿”口径；
   - 按股票最新交易日涨跌幅估算；
   - 新增支持港股持仓，港股代码自动识别为 HK 市场。

2. ETF 联接基金 / 指数联接基金 / FOF
   - 使用 DEFAULT_FUND_PROXY_MAP 或用户传入的 proxy_map 指定底层 ETF / 指数代理；
   - 按底层 ETF / 指数涨跌幅 × 持仓权重估算；
   - 默认不把 ETF 联接基金的底层 ETF 权重归一化到 100%，现金仓位按 0 处理；
   - 如果你想把底层代理权重也归一化，可以设置 proxy_normalize_weights=True。

核心输出表
==========
序号 | 基金代码 | 基金名称 | 今日预估涨跌幅 | 限购金额

默认排序
========
按“今日预估涨跌幅”从高到低排序：
- 序号越小，涨幅越高；
- 序号越大，跌幅越大；
- 计算失败的基金排最后。

最推荐调用方式
==============
from tools.get_top10_holdings import estimate_funds_and_save_table

result_df, detail_map = estimate_funds_and_save_table(
    fund_codes=["017437", "007467", "015016", "007722"],
    top_n=10,
    output_file="output/fund_estimate_table.png",
    title=None,
    holding_mode="auto",
    us_realtime=False,
    hk_realtime=True,
    include_purchase_limit=True,
    sort_by_return=True,
    watermark_text="鱼师",
    up_color="red",
    down_color="green",
    print_table=True,
    save_table=True,
)

重要说明
========
1. 基金持仓来自公开披露数据，不是基金实时持仓。
2. 股票型基金默认按“可获取行情的前 N 大股票再次归一化到 100%”后的估算；如果某只持仓行情缺失，会剔除该持仓并在剩余可查持仓中重新分配权重。
3. ETF 联接 / FOF 的估算值默认使用代理资产的原始披露仓位，不强制归一化。
4. QDII 基金会受汇率、估值时点、现金仓位、费用、申赎等影响；本模块只做近似估算。
5. 限购金额来自公开网页文本解析，可能返回“未知”。
6. 本版本新增 JSON 文件缓存：
   - 基金持仓默认 75 天更新一次；
   - 限购金额默认 7 天更新一次；
   - CN/HK 行情默认小时级缓存，US 行情默认日级缓存。
"""

import re
import json
import time
import requests
import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnchoredOffsetbox, HPacker, TextArea

from pathlib import Path
from io import StringIO
from datetime import datetime, timedelta
from matplotlib import font_manager

# ============================================================
# Runtime JSON cache utilities
# ============================================================

CACHE_DIR = Path("cache")
FUND_HOLDINGS_CACHE_FILE = "fund_holdings_cache.json"
FUND_PURCHASE_LIMIT_CACHE_FILE = "fund_purchase_limit_cache.json"
SECURITY_RETURN_CACHE_FILE = "security_return_cache.json"

_SECURITY_RETURN_RUNTIME_CACHE = {}

# 海外股票持仓型基金：有效已披露持仓的人工放大系数。
# 1.50 表示把“行情有效的已披露持仓股占净值比例”放大 50%，
# 同时从纳斯达克100补偿仓位中扣除对应权重，保持总估算权重不超过 100%。
OVERSEAS_VALID_HOLDING_BOOST = 1.50

def _cache_log(message: str) -> None:
    """统一缓存日志输出，便于在 GitHub Actions 中定位。"""
    print(f"[CACHE] {message}", flush=True)


def _ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _load_json_cache(filename: str, default=None):
    """
    读取 cache/*.json。文件不存在或损坏时返回 default。
    """
    if default is None:
        default = {}

    _ensure_cache_dir()
    path = CACHE_DIR / filename

    if not path.exists():
        return default

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(default, dict) and not isinstance(data, dict):
            return default

        return data

    except Exception as e:
        print(f"[WARN] 缓存读取失败: {path}, 原因: {e}", flush=True)
        return default


def _save_json_cache(filename: str, data) -> None:
    """
    保存 cache/*.json。
    """
    _ensure_cache_dir()
    path = CACHE_DIR / filename

    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    tmp_path.replace(path)


def _is_cache_fresh(fetched_at, max_age_days=None, max_age_hours=None) -> bool:
    """
    判断缓存是否仍在有效期内。

    max_age_days:
        日级有效期，例如基金持仓 75 天、限购 7 天。
    max_age_hours:
        小时级有效期，例如 A股/港股盘中行情 1-2 小时。
    """
    if not fetched_at:
        return False

    try:
        t = pd.to_datetime(fetched_at)
    except Exception:
        return False

    if pd.isna(t):
        return False

    now = pd.Timestamp.now()

    # 如果缓存时间带 timezone，而 now 不带 timezone，做一次兼容
    try:
        age_seconds = (now - t).total_seconds()
    except TypeError:
        age_seconds = (now.tz_localize(None) - t.tz_localize(None)).total_seconds()

    if max_age_hours is not None:
        return age_seconds <= float(max_age_hours) * 3600

    if max_age_days is not None:
        return age_seconds <= float(max_age_days) * 86400

    return False


def _df_to_cache_json(df: pd.DataFrame) -> str:
    """
    DataFrame 序列化为 JSON 字符串。
    """
    return df.to_json(
        orient="records",
        force_ascii=False,
        date_format="iso",
    )


def _df_from_cache_json(data_json: str) -> pd.DataFrame:
    """
    从缓存 JSON 字符串恢复 DataFrame。
    """
    return pd.read_json(StringIO(data_json), orient="records")


def _normalize_security_cache_ticker(market, ticker) -> str:
    """
    统一行情缓存中的 ticker 写法。
    """
    market = str(market).strip().upper()

    if market == "CN":
        return str(ticker).strip().zfill(6)

    if market == "HK":
        return normalize_hk_code(ticker)

    if market == "US":
        return str(ticker).strip().upper()

    return str(ticker).strip().upper()


def _security_return_cache_bucket(market, cn_hk_hourly_cache=True) -> tuple[str, float]:
    """
    返回行情缓存时间桶和有效期。

    规则：
        CN/HK：小时级 key，适合 A股交易日盘中估算；
        US：日级 key，因为北京时间运行时美股通常已经收盘。
    """
    market = str(market).strip().upper()
    now = datetime.now()

    if cn_hk_hourly_cache and market in {"CN", "HK"}:
        return now.strftime("%Y-%m-%d-%H"), 2.0

    return now.strftime("%Y-%m-%d"), 36.0


def _security_return_cache_key(market, ticker, cn_hk_hourly_cache=True) -> tuple[str, str, float]:
    """
    生成行情缓存 key。
    """
    market = str(market).strip().upper()
    ticker_norm = _normalize_security_cache_ticker(market, ticker)
    bucket, max_age_hours = _security_return_cache_bucket(
        market=market,
        cn_hk_hourly_cache=cn_hk_hourly_cache,
    )
    return f"{market}:{ticker_norm}:{bucket}", ticker_norm, max_age_hours



# ============================================================
# 0. 用户可修改：ETF 联接 / FOF / 指数基金代理映射
# ============================================================

DEFAULT_FUND_PROXY_MAP = {
    # 华泰柏瑞中证红利低波动 ETF 联接
    # 512890 为你前面一直使用的红利低波华泰 ETF 代码。
    "007467": {
        "description": "华泰柏瑞中证红利低波动ETF联接：用底层 512890 ETF 代理",
        "components": [
            {
                "name": "华泰柏瑞中证红利低波动ETF",
                "code": "512890",
                "type": "cn_etf",
                "weight_pct": 99.31,
            }
        ],
    },

    # 华安国际龙头(DAX)ETF 联接(QDII)
    # 常见场内 DAX ETF 代码可能为 513030；如与你软件显示不一致，请改这里。
    "015016": {
        "description": "华安国际龙头(DAX)ETF联接(QDII)：用底层 DAX ETF 代理",
        "components": [
            {
                "name": "华安德国(DAX)ETF",
                "code": "513030",
                "type": "cn_etf",
                "weight_pct": 99.89,
            }
        ],
    },

    # 天弘标普500(QDII-FOF)C
    # 不再使用指数直连接口；改用 SPY 作为标普500代理，走美股 ETF 行情。
    "007722": {
        "description": "天弘标普500(QDII-FOF)C：用 SPY 代理标普500基金仓位",
        "components": [
            {
                "name": "SPDR标普500ETF",
                "code": "SPY",
                "type": "us_etf",
                "weight_pct": 99.77,
            }
        ],
    },
    # 如果你有其他 ETF 联接 / FOF 需要代理，可以在这里添加，格式同上。
    "015311": {
        "description": "华泰柏瑞南方东英恒生科技指数：用 015311 代理恒生科技指数基金仓位",
        "components": [
            {
                "name": "恒生科技指数基金",
                "code": "513130",
                "type": "cn_etf",
                "weight_pct": 99.17,
            }
        ],
    },
    "005125": {
        "description": "华宝标普中国A股红利指数：用 005125 代理标普中国A股红利指数基金仓位",
        "components": [
            {
                "name": "标普中国A股红利指数基金",
                "code": "562060",
                "type": "cn_etf",
                "weight_pct": 99.17,
            }
        ],
    },
    "020713": {
        "description": "华安三菱日联日经225ETF：用 020713 代理日经225指数基金仓位",
        "components": [
            {
                "name": "日经225指数基金",
                "code": "513880",
                "type": "cn_etf",
                "weight_pct": 99.17,
            }
        ],
    },
    "023918": {
        "description": "华夏国证自由现金流：用 023918 代理自由现金流指数基金仓位",
        "components": [
            {
                "name": "自由现金流指数基金",
                "code": "159201",
                "type": "cn_etf",
                "weight_pct": 99.17,
            }
        ],
    },
    "008987": {
        "description": "广发上海金ETF：用 008987 代理上海金指数基金仓位",
        "components": [
            {
                "name": "上海金指数基金",
                "code": "518600",
                "type": "cn_etf",
                "weight_pct": 99.17,
            }
        ],
    },
    "016020": {
        "description": "招商中证电池主题ETF：用 016020 代理电池主题指数基金仓位",
        "components": [
            {
                "name": "电池主题指数基金",
                "code": "561910",
                "type": "cn_etf",
                "weight_pct": 99.17,
            }
        ],
    },
    "012725": {
        "description": "国泰中证畜牧养殖：用 012725 代理畜牧养殖指数基金仓位",
        "components": [
            {
                "name": "畜牧养殖指数基金",
                "code": "159865",
                "type": "cn_etf",
                "weight_pct": 99.17,
            }
        ],
    },
    "023145": {
        "description": "汇添富中证油气资源：用 023145 代理油气资源指数基金仓位",
        "components": [
            {
                "name": "油气资源指数基金",
                "code": "159309",
                "type": "cn_etf",
                "weight_pct": 99.17,
            }
        ],
    },
}


# ============================================================
# 1. Matplotlib 中文字体
# ============================================================

_CHINESE_FONT_READY = False


def setup_chinese_font(force=False):
    """
    设置 Matplotlib 中文字体，避免表格图片中文乱码。

    参数
    ----
    force : bool
        False：如果已经设置过字体，则不重复设置。
        True ：强制重新扫描字体。
    """
    global _CHINESE_FONT_READY

    if _CHINESE_FONT_READY and not force:
        return

    candidate_font_paths = [
        r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\msyhbd.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\simsun.ttc",
        r"C:\Windows\Fonts\Deng.ttf",
        "/System/Library/Fonts/PingFang.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKsc-Regular.otf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    ]

    for font_path in candidate_font_paths:
        fp = Path(font_path)
        if fp.exists():
            try:
                font_manager.fontManager.addfont(str(fp))
            except Exception:
                pass

    candidate_font_names = [
        "Microsoft YaHei",
        "SimHei",
        "SimSun",
        "DengXian",
        "Noto Sans CJK SC",
        "Noto Sans CJK JP",
        "Source Han Sans SC",
        "WenQuanYi Micro Hei",
        "WenQuanYi Zen Hei",
        "PingFang SC",
        "Arial Unicode MS",
    ]

    available_font_names = {font.name for font in font_manager.fontManager.ttflist}

    chosen = None
    for name in candidate_font_names:
        if name in available_font_names:
            chosen = name
            break

    if chosen:
        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = [chosen, *candidate_font_names, "DejaVu Sans"]
    else:
        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = [*candidate_font_names, "DejaVu Sans"]

    plt.rcParams["axes.unicode_minus"] = False
    _CHINESE_FONT_READY = True


# ============================================================
# 2. 基金名称与限购信息
# ============================================================

_FUND_NAME_CACHE = None
_FUND_LIMIT_CACHE = {}


def get_fund_name(fund_code: str) -> str:
    """
    根据基金代码查询基金简称。

    参数
    ----
    fund_code : str
        基金代码，例如 "017437"。

    返回
    ----
    str
        基金简称；失败时返回 "基金xxxxxx"。
    """
    global _FUND_NAME_CACHE

    fund_code = str(fund_code).zfill(6)

    try:
        if _FUND_NAME_CACHE is None:
            _FUND_NAME_CACHE = ak.fund_name_em()

        name_df = _FUND_NAME_CACHE.copy()
        name_df["基金代码"] = name_df["基金代码"].astype(str).str.zfill(6)

        hit = name_df[name_df["基金代码"] == fund_code]

        if not hit.empty:
            return str(hit.iloc[0]["基金简称"])

    except Exception as e:
        print(f"[WARN] 基金名称获取失败: {fund_code}, 原因: {e}")

    return f"基金{fund_code}"


def _normalize_html_text(text):
    """
    粗略压缩 HTML 文本，方便正则匹配。
    """
    text = re.sub(r"<script.*?</script>", "", text, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", "", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("&nbsp;", "")
    text = text.replace("&amp;", "&")
    text = re.sub(r"\s+", "", text)
    return text


def get_fund_purchase_limit_uncached(fund_code: str, timeout=8) -> str:
    """
    尝试获取基金当前限购金额。

    参数
    ----
    fund_code : str
        基金代码，例如 "017437"。
    timeout : int or float
        单个网页请求超时时间，单位秒。

    返回
    ----
    str
        常见返回值：
            "100元"
            "1000元"
            "1万元"
            "暂停申购"
            "限购(未识别金额)"
            "不限购/开放申购"
            "未知"

    说明
    ----
    限购信息来自公开网页文本解析。不同基金页面结构不同，结果可能为“未知”。
    """
    global _FUND_LIMIT_CACHE

    fund_code = str(fund_code).zfill(6)

    if fund_code in _FUND_LIMIT_CACHE:
        return _FUND_LIMIT_CACHE[fund_code]

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Referer": f"https://fund.eastmoney.com/{fund_code}.html",
    }

    urls = [
        f"https://fund.eastmoney.com/{fund_code}.html",
        f"https://fundf10.eastmoney.com/jbgk_{fund_code}.html",
        f"https://fundf10.eastmoney.com/jjfl_{fund_code}.html",
        f"https://fundf10.eastmoney.com/jjjz_{fund_code}.html",
    ]

    amount_patterns = [
        r"单日累计购买上限[:：]?(?:为)?(\d+(?:\.\d+)?(?:万)?元)",
        r"单日申购上限[:：]?(?:为)?(\d+(?:\.\d+)?(?:万)?元)",
        r"申购上限[:：]?(?:为)?(\d+(?:\.\d+)?(?:万)?元)",
        r"限购金额[:：]?(?:为)?(\d+(?:\.\d+)?(?:万)?元)",
        r"大额申购.*?(\d+(?:\.\d+)?(?:万)?元)",
        r"单个基金账户.*?累计.*?(\d+(?:\.\d+)?(?:万)?元)",
        r"每个基金账户.*?累计.*?(\d+(?:\.\d+)?(?:万)?元)",
    ]

    result = "未知"

    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()

            if not resp.encoding:
                resp.encoding = resp.apparent_encoding or "utf-8"

            clean_text = _normalize_html_text(resp.text)

            found_amount = None
            for pattern in amount_patterns:
                m = re.search(pattern, clean_text, flags=re.S)
                if m:
                    found_amount = m.group(1)
                    break

            if found_amount:
                result = found_amount
                break

            if "暂停申购" in clean_text and "开放申购" not in clean_text:
                result = "暂停申购"
                break

            if "暂停大额申购" in clean_text or "限制大额申购" in clean_text:
                result = "限购(未识别金额)"
                break

            if "开放申购" in clean_text:
                result = "不限购/开放申购"
                break

        except Exception:
            continue

    _FUND_LIMIT_CACHE[fund_code] = result
    return result


def get_fund_purchase_limit(
    fund_code: str,
    timeout=8,
    cache_days=7,
    cache_enabled=True,
) -> str:
    """
    获取基金限购金额，带文件缓存。

    设计：
        - 默认 7 天更新一次；
        - GitHub Actions 中配合提交 cache/*.json 回仓库，可跨任务复用；
        - 如果更新失败且旧缓存存在，优先使用旧缓存。
    """
    fund_code = str(fund_code).zfill(6)

    if not cache_enabled:
        return get_fund_purchase_limit_uncached(fund_code=fund_code, timeout=timeout)

    cache = _load_json_cache(FUND_PURCHASE_LIMIT_CACHE_FILE, default={})
    item = cache.get(fund_code)

    if item and _is_cache_fresh(item.get("fetched_at"), max_age_days=cache_days):
        value = item.get("value", "未知")
        _FUND_LIMIT_CACHE[fund_code] = value
        _cache_log(f"使用限购缓存: {fund_code} -> {value}")
        return value

    old_value = item.get("value") if isinstance(item, dict) else None

    try:
        _cache_log(f"重新获取限购信息: {fund_code}")
        value = get_fund_purchase_limit_uncached(
            fund_code=fund_code,
            timeout=timeout,
        )

        # 如果本次只得到“未知”，但旧缓存有明确值，则保留旧值，避免网络异常污染缓存。
        if value == "未知" and old_value and old_value != "未知":
            print(f"[WARN] 限购新结果为未知，继续沿用旧缓存: {fund_code} -> {old_value}", flush=True)
            return old_value

        cache[fund_code] = {
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "value": value,
        }
        _save_json_cache(FUND_PURCHASE_LIMIT_CACHE_FILE, cache)

        _FUND_LIMIT_CACHE[fund_code] = value
        return value

    except Exception as e:
        if old_value:
            print(f"[WARN] 限购更新失败，使用旧缓存: {fund_code}, 原因: {e}", flush=True)
            return old_value

        print(f"[WARN] 限购获取失败且无缓存: {fund_code}, 原因: {e}", flush=True)
        return "未知"


# ============================================================
# 3. 字段排序、股票代码识别和通用工具
# ============================================================

def quarter_key(q):
    """
    把类似 '2026年1季度股票投资明细' 的字段转成 20261，方便排序。
    """
    text = str(q)

    year_match = re.search(r"(\d{4})", text)
    quarter_match = re.search(r"([1-4])\s*季度", text)

    if not year_match:
        return -1

    year = int(year_match.group(1))
    quarter = int(quarter_match.group(1)) if quarter_match else 0

    return year * 10 + quarter


US_TICKER_MAP = {
    "奈飞": "NFLX",
    "Netflix": "NFLX",
    "英伟达": "NVDA",
    "NVIDIA": "NVDA",
    "苹果": "AAPL",
    "Apple": "AAPL",
    "微软": "MSFT",
    "Microsoft": "MSFT",
    "博通": "AVGO",
    "Broadcom": "AVGO",
    "特斯拉": "TSLA",
    "Tesla": "TSLA",
    "谷歌-C": "GOOG",
    "谷歌C": "GOOG",
    "Alphabet Inc Class C": "GOOG",
    "谷歌-A": "GOOGL",
    "谷歌A": "GOOGL",
    "Alphabet Inc Class A": "GOOGL",
    "亚马逊": "AMZN",
    "Amazon": "AMZN",
    "Meta Platforms Inc-A": "META",
    "Meta Platforms": "META",
    "Meta": "META",
    "迈威尔科技": "MRVL",
    "迈威尔": "MRVL",
    "Marvell": "MRVL",
    "台积电": "TSM",
    "TSMC": "TSM",
    "康宁": "GLW",
    "Corning": "GLW",
}


def normalize_hk_code(code):
    """
    规范化港股代码。

    支持输入：
        700
        "700"
        "00700"
        "0700.HK"
        "00700.HK"
        "HK00700"
        "hk00700"

    返回：
        "00700"
    """
    text = str(code).strip().upper()

    text = text.replace(".HK", "")
    text = text.replace("HK", "")

    digits = re.sub(r"\D", "", text)

    if not digits:
        raise RuntimeError(f"无法从港股代码中提取数字: {code}")

    if len(digits) > 5:
        digits = digits[-5:]

    return digits.zfill(5)


def detect_market_and_ticker(raw_code, stock_name):
    """
    识别股票市场和行情代码。

    返回
    ----
    tuple[str, str]
        market, ticker

    market 取值
    ----------
    US:
        美股。使用美股行情接口获取。
    CN:
        A股。使用新浪实时行情获取。
    HK:
        港股。使用港股行情接口获取。
    UNKNOWN:
        未识别。
    """
    raw = str(raw_code).strip()
    name = str(stock_name).strip()
    raw_upper = raw.upper()

    # 1. 港股代码：00700.HK / 0700.HK / HK00700
    if re.match(r"^(HK)?\d{1,5}(\.HK)?$", raw_upper) and not re.match(r"^\d{6}$", raw_upper):
        try:
            return "HK", normalize_hk_code(raw_upper)
        except Exception:
            pass

    # 2. 美股 ticker：NFLX, NVDA, AAPL, TSM, GOOGL, LITE
    if re.match(r"^[A-Z]{1,8}$", raw_upper):
        return "US", raw_upper

    # 3. 美股带后缀：NFLX.O, NVDA.O
    if re.match(r"^[A-Z]{1,8}\.[A-Z]+$", raw_upper) and not raw_upper.endswith(".HK"):
        return "US", raw_upper.split(".")[0]

    # 4. A股 6 位数字
    if re.match(r"^\d{6}$", raw):
        return "CN", raw

    # 5. 港股 1-5 位纯数字兜底
    if re.match(r"^\d{1,5}$", raw):
        return "HK", normalize_hk_code(raw)

    # 6. 名称映射兜底
    for key, ticker in US_TICKER_MAP.items():
        if key in name:
            return "US", ticker

    return "UNKNOWN", raw


def _to_float_safe(value):
    """
    安全转换数值，兼容 '1.23%'、'1,234.56'、'--' 等形式。
    """
    if value is None:
        return None

    text = str(value).strip()

    if text in {"", "-", "--", "None", "nan", "NaN"}:
        return None

    text = text.replace("%", "").replace(",", "")

    try:
        return float(text)
    except Exception:
        return None


def _pick_column(df, candidates):
    """
    从 DataFrame 中选择第一个存在的候选列。
    """
    for col in candidates:
        if col in df.columns:
            return col

    return None


def _last_us_symbol(code):
    """
    将常见美股代码格式归一成 ticker。

    例：
        105.NVDA -> NVDA
        gb_aapl  -> AAPL
        AAPL     -> AAPL
    """
    text = str(code).strip().upper()

    if "." in text:
        tail = text.split(".")[-1]
        if re.match(r"^[A-Z]{1,8}$", tail):
            return tail

    if "_" in text:
        tail = text.split("_")[-1]
        if re.match(r"^[A-Z]{1,8}$", tail):
            return tail

    return text


def _match_us_row(df, ticker):
    """
    在美股实时行情表中匹配 ticker。
    """
    ticker = str(ticker).strip().upper()

    code_cols = [
        "代码",
        "股票代码",
        "symbol",
        "Symbol",
        "SYMBOL",
        "code",
        "Code",
        "标识",
    ]

    code_col = _pick_column(df, code_cols)

    if code_col is None:
        raise RuntimeError(f"美股行情表缺少代码列，当前列={list(df.columns)}")

    tmp = df.copy()
    tmp["_ticker_norm"] = tmp[code_col].astype(str).map(_last_us_symbol)

    hit = tmp[tmp["_ticker_norm"] == ticker]

    if hit.empty:
        return None

    return hit.iloc[0]


def format_pct(value, digits=4):
    """
    百分数格式化。
    """
    if value is None or pd.isna(value):
        return "计算失败"

    return f"{float(value):+.{digits}f}%"


def _normalize_valuation_mode(valuation_mode):
    """
    统一估值口径。

    intraday:
        A股/港股尽量使用盘中实时；美股默认使用最新完整交易日日线。
    last_close:
        A股、港股、美股全部使用最新完整交易日日线。
        适合 QDII / 全球投资基金，避免把昨夜美股和今日 A/H 盘中混在一起。
    auto:
        股票持仓估算时按持仓市场自动判断：
        - 含 US 持仓：使用 last_close；
        - 不含 US 且为 CN/HK 持仓：使用 intraday。
        这样纯港股基金继续走港股实时，全球跨市场基金走统一收盘口径。
    """
    mode = str(valuation_mode or "intraday").strip().lower()
    aliases = {
        "realtime": "intraday",
        "real_time": "intraday",
        "live": "intraday",
        "t+0": "intraday",
        "close": "last_close",
        "daily": "last_close",
        "lastclose": "last_close",
        "last_close": "last_close",
        "t+1": "last_close",
        "automatic": "auto",
        "smart": "auto",
    }
    mode = aliases.get(mode, mode)
    if mode not in {"intraday", "last_close", "auto"}:
        raise ValueError("valuation_mode 只能是 'intraday'、'last_close' 或 'auto'")
    return mode


def _resolve_auto_valuation_mode_from_markets(markets):
    """
    根据持仓市场决定 auto 估值口径。

    规则：
        - 只要含 US，就使用 last_close，避免美股收盘和 A/H 盘中混算；
        - 不含 US 的 CN/HK 组合使用 intraday；
        - UNKNOWN 不改变判断，尽量由可识别市场决定。
    """
    market_set = {str(x).strip().upper() for x in markets if str(x).strip()}
    if "US" in market_set:
        return "last_close"
    return "intraday"


def _component_market_type(component):
    """
    从代理组件 type 推断市场。
    """
    ctype = str(component.get("type", "")).strip().lower()
    if ctype in {"us_ticker", "us_stock", "us_etf"}:
        return "US"
    if ctype in {"hk_stock", "hk_etf", "hk_security"}:
        return "HK"
    if ctype in {"cn_etf", "cn_stock", "cn_security", "cn_fund"}:
        return "CN"
    return "UNKNOWN"


def _resolve_auto_valuation_mode_from_components(components):
    """
    根据代理组件市场决定 auto 估值口径。
    """
    return _resolve_auto_valuation_mode_from_markets(
        [_component_market_type(x) for x in components]
    )

# ============================================================
# 4. 行情接口
# ============================================================

_US_SPOT_SINA_CACHE = None
_US_SPOT_EM_CACHE = None
_HK_SPOT_EM_CACHE = None


def infer_sina_cn_symbol(code):
    """
    根据 A 股 / ETF / 场内基金代码推断新浪 symbol。

    常见规则：
        5xxxxx, 6xxxxx, 688xxx -> sh
        0xxxxx, 1xxxxx, 2xxxxx, 3xxxxx -> sz
    """
    code = str(code).strip().zfill(6)

    if code.startswith(("5", "6", "9")) or code.startswith("688"):
        return "sh" + code

    if code.startswith(("0", "1", "2", "3")):
        return "sz" + code

    raise RuntimeError(f"无法识别沪深交易所前缀: {code}")


def fetch_cn_security_return_pct(code, retry=2, sleep_seconds=0.8):
    """
    获取 A股 / A股ETF / 场内基金实时涨跌幅，返回百分数。

    实时逻辑：
        最新价 / 昨收价 - 1
    """
    code = str(code).zfill(6)
    sina_symbol = infer_sina_cn_symbol(code)
    url = f"https://hq.sinajs.cn/list={sina_symbol}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Referer": "https://finance.sina.com.cn/",
    }

    last_error = None

    for i in range(max(1, retry)):
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = "gbk"

            text = resp.text.strip()
            m = re.search(r'="(.*)"', text)

            if not m:
                raise RuntimeError(f"新浪返回格式异常: {text[:100]}")

            values = m.group(1).split(",")

            if len(values) < 32:
                raise RuntimeError(f"新浪字段数量异常: len={len(values)}")

            prev_close = pd.to_numeric(values[2], errors="coerce")
            latest_price = pd.to_numeric(values[3], errors="coerce")

            if pd.isna(latest_price) or pd.isna(prev_close) or float(prev_close) == 0:
                raise RuntimeError(f"新浪价格无效: latest={latest_price}, prev={prev_close}")

            return (float(latest_price) / float(prev_close) - 1.0) * 100.0, "sina_realtime"

        except Exception as e:
            last_error = e

            if i < max(1, retry) - 1:
                time.sleep(sleep_seconds)

    raise RuntimeError(f"新浪行情失败: {code}, 原因: {last_error}")


def fetch_cn_security_return_pct_daily(code, lookback_days=90):
    """
    获取 A股 / A股ETF / 场内基金最新完整交易日日线涨跌幅，返回百分数。

    valuation_mode="last_close" 使用此函数，避免跨市场基金把昨夜美股和今日 A/H 盘中行情混在一起。
    """
    code = str(code).strip().zfill(6)
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y%m%d")

    errors = []
    frames = []

    try:
        df = ak.fund_etf_hist_em(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="",
        )
        if df is not None and not df.empty:
            frames.append((df, "ak_fund_etf_hist_em"))
    except Exception as e:
        errors.append(f"fund_etf_hist_em: {repr(e)}")

    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="",
        )
        if df is not None and not df.empty:
            frames.append((df, "ak_stock_zh_a_hist"))
    except Exception as e:
        errors.append(f"stock_zh_a_hist: {repr(e)}")

    if not frames:
        raise RuntimeError(f"A股/场内基金日线返回空数据: {code}; {' | '.join(errors)}")

    for raw_df, source_name in frames:
        out = raw_df.copy()
        date_col = _pick_column(out, ["日期", "date", "Date"])
        close_col = _pick_column(out, ["收盘", "close", "Close", "收盘价"])
        pct_col = _pick_column(out, ["涨跌幅", "涨幅", "pct_chg", "change_percent", "ChangePercent"])

        if date_col is not None:
            out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
            out = out.dropna(subset=[date_col]).sort_values(date_col)

        if out.empty:
            continue

        if pct_col is not None:
            pct_series = pd.to_numeric(out[pct_col], errors="coerce").dropna()
            if not pct_series.empty:
                return float(pct_series.iloc[-1]), f"{source_name}_pct"

        if close_col is None:
            continue

        out[close_col] = pd.to_numeric(out[close_col], errors="coerce")
        out = out.dropna(subset=[close_col])

        if len(out) >= 2:
            last_close = float(out.iloc[-1][close_col])
            prev_close = float(out.iloc[-2][close_col])
            if prev_close != 0:
                return (last_close / prev_close - 1.0) * 100.0, f"{source_name}_close_calc"

    raise RuntimeError(f"无法解析 A股/场内基金 {code} 日线涨跌幅; {' | '.join(errors)}")


def fetch_hk_return_pct_akshare_daily(code, lookback_days=90):
    """
    使用 AKShare 港股历史日线获取最新交易日涨跌幅。

    逻辑：
        优先读取历史日线中的“涨跌幅”列；
        如果没有“涨跌幅”列，则使用最新收盘价 / 前一交易日收盘价 - 1。
    """
    hk_code = normalize_hk_code(code)

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y%m%d")

    try:
        df = ak.stock_hk_hist(
            symbol=hk_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="",
        )
    except TypeError:
        # 兼容少数 AKShare 版本参数签名差异
        df = ak.stock_hk_hist(
            symbol=hk_code,
            period="daily",
            adjust="",
        )

    if df is None or df.empty:
        raise RuntimeError(f"stock_hk_hist 返回空数据: {hk_code}")

    out = df.copy()

    date_col = _pick_column(out, ["日期", "date", "Date"])
    close_col = _pick_column(out, ["收盘", "close", "Close", "收盘价"])
    pct_col = _pick_column(out, ["涨跌幅", "涨幅", "pct_chg", "change_percent", "ChangePercent"])

    if date_col is not None:
        out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        out = out.dropna(subset=[date_col])
        out = out.sort_values(date_col)

    if out.empty:
        raise RuntimeError(f"stock_hk_hist 有效数据为空: {hk_code}")

    # AKShare 港股历史数据通常有“涨跌幅”列，单位为百分数。
    if pct_col is not None:
        pct_series = pd.to_numeric(out[pct_col], errors="coerce").dropna()
        if not pct_series.empty:
            return float(pct_series.iloc[-1]), "ak_stock_hk_hist_pct"

    if close_col is None:
        raise RuntimeError(f"stock_hk_hist 缺少收盘价列: {hk_code}; 当前列={list(out.columns)}")

    out[close_col] = pd.to_numeric(out[close_col], errors="coerce")
    out = out.dropna(subset=[close_col])

    if len(out) < 2:
        raise RuntimeError(f"stock_hk_hist 有效收盘价数量不足: {hk_code}")

    last_close = float(out.iloc[-1][close_col])
    prev_close = float(out.iloc[-2][close_col])

    if prev_close == 0:
        raise RuntimeError(f"stock_hk_hist 前一交易日收盘价为 0: {hk_code}")

    return (last_close / prev_close - 1.0) * 100.0, "ak_stock_hk_hist_close_calc"


def _normalize_hk_symbol_for_match(value):
    """
    统一港股行情表中的代码格式，用于匹配。

    支持：
        700
        00700
        HK00700
        00700.HK

    返回：
        00700
    """
    text = str(value).strip().upper()
    text = text.replace(".HK", "")
    text = text.replace("HK", "")

    digits = re.sub(r"\D", "", text)

    if not digits:
        return ""

    if len(digits) > 5:
        digits = digits[-5:]

    return digits.zfill(5)


def _match_hk_row(df, hk_code):
    """
    在 AKShare 港股实时行情表中匹配指定港股代码。
    """
    hk_code = normalize_hk_code(hk_code)

    code_cols = [
        "代码",
        "股票代码",
        "symbol",
        "Symbol",
        "SYMBOL",
        "code",
        "Code",
    ]

    code_col = _pick_column(df, code_cols)

    if code_col is None:
        raise RuntimeError(f"港股实时行情表缺少代码列，当前列={list(df.columns)}")

    tmp = df.copy()
    tmp["_hk_code_norm"] = tmp[code_col].astype(str).map(_normalize_hk_symbol_for_match)

    hit = tmp[tmp["_hk_code_norm"] == hk_code]

    if hit.empty:
        return None

    return hit.iloc[0]


def fetch_hk_return_pct_akshare_spot_em(code):
    """
    使用 AKShare 东方财富港股实时行情获取当日涨跌幅。

    逻辑：
        1. 通过 ak.stock_hk_spot_em() 拉取港股实时行情表；
        2. 按港股代码匹配；
        3. 优先读取“涨跌幅”列；
        4. 如果没有“涨跌幅”列，则用 最新价 / 昨收价 - 1 计算。

    返回：
        return_pct, source
    """
    global _HK_SPOT_EM_CACHE

    hk_code = normalize_hk_code(code)

    if _HK_SPOT_EM_CACHE is None:
        _HK_SPOT_EM_CACHE = ak.stock_hk_spot_em()

    df = _HK_SPOT_EM_CACHE

    if df is None or df.empty:
        raise RuntimeError("ak.stock_hk_spot_em 返回空数据")

    row = _match_hk_row(df, hk_code)

    if row is None:
        raise RuntimeError(f"stock_hk_spot_em 未找到港股 {hk_code}; 当前列={list(df.columns)}")

    pct_col = _pick_column(
        df,
        [
            "涨跌幅",
            "涨幅",
            "changePercent",
            "ChangePercent",
            "pct_chg",
            "change_percent",
            "涨跌幅%",
        ],
    )

    if pct_col is not None:
        pct = _to_float_safe(row.get(pct_col))
        if pct is not None:
            return float(pct), "ak_stock_hk_spot_em"

    price_col = _pick_column(
        df,
        [
            "最新价",
            "最新",
            "现价",
            "price",
            "Price",
            "last",
            "Last",
            "收盘价",
        ],
    )

    prev_col = _pick_column(
        df,
        [
            "昨收价",
            "昨收",
            "previousClose",
            "PreviousClose",
            "prev_close",
            "昨收盘",
        ],
    )

    if price_col is not None and prev_col is not None:
        price = _to_float_safe(row.get(price_col))
        prev = _to_float_safe(row.get(prev_col))

        if price is not None and prev not in (None, 0):
            return (float(price) / float(prev) - 1.0) * 100.0, "ak_stock_hk_spot_em_calc"

    raise RuntimeError(
        f"stock_hk_spot_em 无法解析港股 {hk_code} 涨跌幅；当前列={list(df.columns)}"
    )


def fetch_hk_return_pct_sina(code, retry=2, sleep_seconds=0.8):
    """
    使用新浪港股单只股票实时行情获取涨跌幅。

    安全逻辑：
        - 不使用字段猜测；
        - 只使用明确价格字段计算：最新价 / 昨收价 - 1；
        - 如果 latest / prev_close 不能可靠解析，直接失败；
        - 上游 fetch_hk_return_pct() 再尝试东方财富实时与港股日线兜底。
    """
    hk_code = normalize_hk_code(code)
    sina_symbol = "hk" + hk_code
    url = f"https://hq.sinajs.cn/list={sina_symbol}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Referer": "https://finance.sina.com.cn/",
    }

    last_error = None

    for i in range(max(1, retry)):
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = "gbk"

            text = resp.text.strip()
            m = re.search(r'="(.*)"', text)

            if not m:
                raise RuntimeError(f"新浪港股返回格式异常: {text[:120]}")

            raw = m.group(1)

            if not raw:
                raise RuntimeError(f"新浪港股返回空内容: {hk_code}")

            values = raw.split(",")

            if len(values) < 8:
                raise RuntimeError(f"新浪港股字段数量不足: len={len(values)}, raw={values[:12]}")

            # 新浪港股常见字段顺序：
            # 0 名称；1 今日开盘价；2 昨日收盘价；3 最高价；4 最低价；5 当前价 / 最新价。
            prev_close = _to_float_safe(values[2])
            latest_price = _to_float_safe(values[5])

            if latest_price is None or prev_close is None:
                raise RuntimeError(
                    f"新浪港股价格字段解析失败: {hk_code}, "
                    f"prev_close_raw={values[2] if len(values) > 2 else None}, "
                    f"latest_raw={values[5] if len(values) > 5 else None}, "
                    f"raw_head={values[:12]}"
                )

            if float(prev_close) <= 0 or float(latest_price) <= 0:
                raise RuntimeError(
                    f"新浪港股价格字段无效: {hk_code}, "
                    f"latest={latest_price}, prev_close={prev_close}, raw_head={values[:12]}"
                )

            return_pct = (float(latest_price) / float(prev_close) - 1.0) * 100.0

            if abs(return_pct) > 40:
                raise RuntimeError(
                    f"新浪港股计算涨跌幅异常: {hk_code}, "
                    f"return_pct={return_pct:.4f}%, "
                    f"latest={latest_price}, prev_close={prev_close}, raw_head={values[:12]}"
                )

            return return_pct, "sina_hk_realtime_price_calc"

        except Exception as e:
            last_error = e
            if i < max(1, retry) - 1:
                time.sleep(sleep_seconds)

    raise RuntimeError(f"新浪港股行情失败: {hk_code}, 原因: {last_error}")


def fetch_hk_return_pct(code, hk_realtime=False):
    """
    获取港股涨跌幅，返回百分数。

    hk_realtime=True：
        新浪港股实时安全解析 -> 东方财富港股实时兜底 -> 港股历史日线兜底。
    hk_realtime=False：
        只使用港股历史日线。
    """
    hk_code = normalize_hk_code(code)
    errors = []

    if hk_realtime:
        try:
            return fetch_hk_return_pct_sina(hk_code)
        except Exception as e:
            errors.append(f"sina_hk_price_calc: {repr(e)}")

        try:
            return fetch_hk_return_pct_akshare_spot_em(hk_code)
        except Exception as e:
            errors.append(f"ak_hk_spot_em: {repr(e)}")

        try:
            return fetch_hk_return_pct_akshare_daily(hk_code)
        except Exception as e:
            errors.append(f"ak_hk_daily: {repr(e)}")

    else:
        try:
            return fetch_hk_return_pct_akshare_daily(hk_code)
        except Exception as e:
            errors.append(f"ak_hk_daily: {repr(e)}")

    raise RuntimeError(f"无法获取港股 {hk_code} 涨跌幅: {' | '.join(errors)}")


def fetch_hk_return_pct_last_close_with_fallback(code):
    """
    港股统一收盘口径的安全获取。

    优先使用港股历史日线；如日线接口异常，为避免整只基金失败，
    允许回退到港股实时安全解析，并在 source 中标记 fallback_intraday。
    """
    hk_code = normalize_hk_code(code)
    try:
        return fetch_hk_return_pct_akshare_daily(hk_code)
    except Exception as e_daily:
        try:
            r_pct, source = fetch_hk_return_pct(hk_code, hk_realtime=True)
            return r_pct, f"{source}_fallback_intraday_after_daily_fail"
        except Exception as e_rt:
            raise RuntimeError(
                f"港股 {hk_code} 日线和实时兜底均失败: daily={e_daily}; realtime={e_rt}"
            )

def fetch_us_return_pct_akshare_daily(ticker):
    """
    使用 AKShare 美股历史日线获取最新交易日涨跌幅。

    逻辑：
        最新交易日收盘价 / 前一交易日收盘价 - 1
    """
    ticker = str(ticker).strip().upper()

    df = ak.stock_us_daily(symbol=ticker, adjust="")

    if df is None or df.empty:
        raise RuntimeError(f"stock_us_daily 返回空数据: {ticker}")

    out = df.copy()

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date"])

    if "close" not in out.columns:
        raise RuntimeError(f"stock_us_daily 缺少 close 列: {ticker}; 当前列={list(out.columns)}")

    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["close"])

    if "date" in out.columns:
        out = out.sort_values("date")

    if len(out) < 2:
        raise RuntimeError(f"stock_us_daily 有效 close 数量不足: {ticker}")

    last_close = float(out.iloc[-1]["close"])
    prev_close = float(out.iloc[-2]["close"])

    if prev_close == 0:
        raise RuntimeError(f"stock_us_daily 前一交易日收盘价为 0: {ticker}")

    return (last_close / prev_close - 1.0) * 100.0, "ak_stock_us_daily"


def fetch_us_return_pct_akshare_spot_sina(ticker):
    """
    使用 AKShare 新浪美股实时行情获取涨跌幅。

    注意：
        ak.stock_us_spot() 会拉较大的美股列表，速度可能较慢。
    """
    global _US_SPOT_SINA_CACHE

    ticker = str(ticker).strip().upper()

    if _US_SPOT_SINA_CACHE is None:
        _US_SPOT_SINA_CACHE = ak.stock_us_spot()

    df = _US_SPOT_SINA_CACHE

    if df is None or df.empty:
        raise RuntimeError("ak.stock_us_spot 返回空数据")

    row = _match_us_row(df, ticker)

    if row is None:
        raise RuntimeError(f"stock_us_spot 未找到 {ticker}; 当前列={list(df.columns)}")

    pct_col = _pick_column(
        df,
        ["涨跌幅", "涨幅", "changePercent", "ChangePercent", "pct_chg", "change_percent", "涨跌幅%"],
    )

    if pct_col is not None:
        pct = _to_float_safe(row.get(pct_col))
        if pct is not None:
            return pct, "ak_stock_us_spot_sina"

    price_col = _pick_column(df, ["最新价", "最新", "现价", "price", "Price", "last", "Last", "收盘价"])
    prev_col = _pick_column(df, ["昨收价", "昨收", "previousClose", "PreviousClose", "prev_close", "昨收盘"])

    if price_col is not None and prev_col is not None:
        price = _to_float_safe(row.get(price_col))
        prev = _to_float_safe(row.get(prev_col))

        if price is not None and prev not in (None, 0):
            return (price / prev - 1.0) * 100.0, "ak_stock_us_spot_sina_calc"

    raise RuntimeError(f"stock_us_spot 无法解析 {ticker} 涨跌幅；当前列={list(df.columns)}")


def fetch_us_return_pct_akshare_spot_em(ticker):
    """
    使用 AKShare 东方财富美股实时行情获取涨跌幅。
    """
    global _US_SPOT_EM_CACHE

    ticker = str(ticker).strip().upper()

    if _US_SPOT_EM_CACHE is None:
        _US_SPOT_EM_CACHE = ak.stock_us_spot_em()

    df = _US_SPOT_EM_CACHE

    if df is None or df.empty:
        raise RuntimeError("ak.stock_us_spot_em 返回空数据")

    row = _match_us_row(df, ticker)

    if row is None:
        raise RuntimeError(f"stock_us_spot_em 未找到 {ticker}; 当前列={list(df.columns)}")

    pct_col = _pick_column(
        df,
        ["涨跌幅", "涨幅", "changePercent", "ChangePercent", "pct_chg", "change_percent", "涨跌幅%"],
    )

    if pct_col is not None:
        pct = _to_float_safe(row.get(pct_col))
        if pct is not None:
            return pct, "ak_stock_us_spot_em"

    price_col = _pick_column(df, ["最新价", "最新", "现价", "price", "Price", "last", "Last"])
    prev_col = _pick_column(df, ["昨收价", "昨收", "previousClose", "PreviousClose", "prev_close", "昨收盘"])

    if price_col is not None and prev_col is not None:
        price = _to_float_safe(row.get(price_col))
        prev = _to_float_safe(row.get(prev_col))

        if price is not None and prev not in (None, 0):
            return (price / prev - 1.0) * 100.0, "ak_stock_us_spot_em_calc"

    raise RuntimeError(f"stock_us_spot_em 无法解析 {ticker} 涨跌幅；当前列={list(df.columns)}")


def fetch_us_return_pct(
    ticker,
    prefer_intraday=True,
    us_realtime=False,
):
    """
    获取美股 ticker 最新交易日涨跌幅，返回百分数。

    已移除 已移除的外部指数直连 相关接口。

    us_realtime=False：
        默认快速模式，优先使用 ak.stock_us_daily() 获取单只股票日线。
        适合收盘后估算，速度较快。

    us_realtime=True：
        盘中实时模式，优先尝试 AKShare 新浪美股实时行情；
        再尝试 AKShare 东方财富美股实时行情；
        最后回落到 ak.stock_us_daily()。

    prefer_intraday:
        为兼容旧调用保留；当前无 已移除的外部指数直连 接口，因此不再影响逻辑。
    """
    ticker = str(ticker).strip().upper()
    errors = []

    if us_realtime:
        try:
            return fetch_us_return_pct_akshare_spot_sina(ticker)
        except Exception as e:
            errors.append(f"ak_spot_sina: {repr(e)}")

        try:
            return fetch_us_return_pct_akshare_spot_em(ticker)
        except Exception as e:
            errors.append(f"ak_spot_em: {repr(e)}")

        try:
            return fetch_us_return_pct_akshare_daily(ticker)
        except Exception as e:
            errors.append(f"ak_daily: {repr(e)}")

    else:
        try:
            return fetch_us_return_pct_akshare_daily(ticker)
        except Exception as e:
            errors.append(f"ak_daily: {repr(e)}")

        # 默认快速模式下不主动调用 ak.stock_us_spot()，避免拉全市场；
        # 这里只保留东方财富实时作为备用。
        try:
            return fetch_us_return_pct_akshare_spot_em(ticker)
        except Exception as e:
            errors.append(f"ak_spot_em: {repr(e)}")

    raise RuntimeError(f"无法获取美股 {ticker} 涨跌幅: {' | '.join(errors)}")


def get_stock_return_pct(
    market,
    ticker,
    manual_returns_pct=None,
    prefer_intraday=True,
    us_realtime=False,
    hk_realtime=False,
    security_return_cache_enabled=True,
    cn_hk_hourly_cache=True,
    valuation_mode="intraday",
):
    """
    根据 market 自动选择行情接口，并对行情涨跌幅做缓存。

    valuation_mode:
        intraday：A股/港股使用盘中实时；美股默认用最新完整日线。
        last_close：A股、港股、美股全部使用最新完整交易日日线。
                    适合 QDII / 全球投资基金 T+1 估值口径。
    """
    market = str(market).strip().upper()
    valuation_mode = _normalize_valuation_mode(valuation_mode)
    key = str(ticker).strip().upper()

    if manual_returns_pct:
        if key in manual_returns_pct:
            return float(manual_returns_pct[key]), "manual"
        if str(ticker).strip() in manual_returns_pct:
            return float(manual_returns_pct[str(ticker).strip()]), "manual"

    cache_key = None
    ticker_norm = None
    max_age_hours = None

    if security_return_cache_enabled:
        effective_cn_hk_hourly_cache = cn_hk_hourly_cache and valuation_mode == "intraday"
        cache_key, ticker_norm, max_age_hours = _security_return_cache_key(
            market=market,
            ticker=ticker,
            cn_hk_hourly_cache=effective_cn_hk_hourly_cache,
        )
        cache_key = f"{cache_key}:{valuation_mode}"

        if cache_key in _SECURITY_RETURN_RUNTIME_CACHE:
            _cache_log(f"使用本轮内存行情缓存: {cache_key}")
            return _SECURITY_RETURN_RUNTIME_CACHE[cache_key]

        cache = _load_json_cache(SECURITY_RETURN_CACHE_FILE, default={})
        item = cache.get(cache_key)

        if item and _is_cache_fresh(item.get("fetched_at"), max_age_hours=max_age_hours):
            try:
                result = (float(item["return_pct"]), item.get("source", "file_cache"))
                _SECURITY_RETURN_RUNTIME_CACHE[cache_key] = result
                _cache_log(f"使用文件行情缓存: {cache_key} -> {result[0]:+.4f}%")
                return result
            except Exception:
                pass

    if ticker_norm is None:
        ticker_norm = _normalize_security_cache_ticker(market, ticker)

    _cache_log(f"重新获取行情: {market}:{ticker_norm} [{valuation_mode}]")

    if valuation_mode == "last_close":
        if market == "US":
            result = fetch_us_return_pct_akshare_daily(ticker_norm)
        elif market == "CN":
            try:
                result = fetch_cn_security_return_pct_daily(str(ticker_norm).zfill(6))
            except Exception:
                r_pct, source = fetch_cn_security_return_pct(str(ticker_norm).zfill(6))
                result = (r_pct, f"{source}_fallback_intraday_after_daily_fail")
        elif market == "HK":
            result = fetch_hk_return_pct_last_close_with_fallback(ticker_norm)
        else:
            raise RuntimeError(f"未知市场类型: market={market}, ticker={ticker}")
    else:
        if market == "US":
            result = fetch_us_return_pct(
                ticker_norm,
                prefer_intraday=prefer_intraday,
                us_realtime=us_realtime,
            )
        elif market == "CN":
            result = fetch_cn_security_return_pct(str(ticker_norm).zfill(6))
        elif market == "HK":
            result = fetch_hk_return_pct(
                ticker_norm,
                hk_realtime=hk_realtime,
            )
        else:
            raise RuntimeError(f"未知市场类型: market={market}, ticker={ticker}")

    if security_return_cache_enabled and cache_key:
        r_pct, source = result
        cache = _load_json_cache(SECURITY_RETURN_CACHE_FILE, default={})
        cache[cache_key] = {
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "market": market,
            "ticker": ticker_norm,
            "return_pct": float(r_pct),
            "source": source,
            "valuation_mode": valuation_mode,
        }
        _save_json_cache(SECURITY_RETURN_CACHE_FILE, cache)
        _SECURITY_RETURN_RUNTIME_CACHE[cache_key] = result

    return result


def get_proxy_return_pct(
    component,
    manual_returns_pct=None,
    prefer_intraday=True,
    us_realtime=False,
    hk_realtime=False,
    security_return_cache_enabled=True,
    cn_hk_hourly_cache=True,
    valuation_mode="intraday",
):
    """
    获取 ETF 联接 / FOF 代理资产涨跌幅。

    代理资产也进入 security_return_cache.json：
        cn_etf -> CN 小时级缓存；
        hk_etf -> HK 小时级缓存；
        us_etf -> US 日级缓存。
    """
    code = str(component.get("code", "")).strip()
    ctype = str(component.get("type", "")).strip().lower()

    manual_key_candidates = [
        code,
        code.upper(),
        str(component.get("name", "")).strip(),
    ]

    if manual_returns_pct:
        for key in manual_key_candidates:
            if key in manual_returns_pct:
                return float(manual_returns_pct[key]), "manual"

    if ctype == "manual":
        if "return_pct" not in component:
            raise RuntimeError(f"manual 代理缺少 return_pct: {component}")
        return float(component["return_pct"]), "manual_component"

    if ctype in {"cn_etf", "cn_stock", "cn_security", "cn_fund"}:
        return get_stock_return_pct(
            market="CN",
            ticker=code,
            manual_returns_pct=manual_returns_pct,
            prefer_intraday=prefer_intraday,
            us_realtime=us_realtime,
            hk_realtime=hk_realtime,
            security_return_cache_enabled=security_return_cache_enabled,
            cn_hk_hourly_cache=cn_hk_hourly_cache,
            valuation_mode=valuation_mode,
        )

    if ctype in {"us_ticker", "us_stock", "us_etf"}:
        return get_stock_return_pct(
            market="US",
            ticker=code,
            manual_returns_pct=manual_returns_pct,
            prefer_intraday=prefer_intraday,
            us_realtime=us_realtime,
            hk_realtime=hk_realtime,
            security_return_cache_enabled=security_return_cache_enabled,
            cn_hk_hourly_cache=cn_hk_hourly_cache,
            valuation_mode=valuation_mode,
        )

    if ctype in {"hk_stock", "hk_etf", "hk_security"}:
        return get_stock_return_pct(
            market="HK",
            ticker=code,
            manual_returns_pct=manual_returns_pct,
            prefer_intraday=prefer_intraday,
            us_realtime=us_realtime,
            hk_realtime=hk_realtime,
            security_return_cache_enabled=security_return_cache_enabled,
            cn_hk_hourly_cache=cn_hk_hourly_cache,
            valuation_mode=valuation_mode,
        )

    if ctype in {"us_index", "eu_index", "index", "fx"}:
        raise RuntimeError(
            f"当前版本已移除指数直连接口，不能使用 type={ctype!r}。"
            f"请把代理改成可交易 ETF，例如 SPY、VOO、513030。component={component}"
        )

    raise RuntimeError(f"未知代理类型: {ctype}; component={component}")


# ============================================================
# 5. 股票持仓估算
# ============================================================

def get_latest_stock_holdings_df_uncached(fund_code="017437", top_n=10):
    """
    获取基金最新披露季度前 N 大股票持仓。
    """
    current_year = datetime.now().year
    years = [str(current_year), str(current_year - 1)]

    frames = []

    for year in years:
        try:
            df = ak.fund_portfolio_hold_em(
                symbol=str(fund_code),
                date=year,
            )

            if df is not None and not df.empty:
                df = df.copy()
                df["查询年份"] = year
                frames.append(df)

        except Exception as e:
            print(f"[WARN] {fund_code} {year} 股票持仓获取失败: {e}")

    if not frames:
        raise RuntimeError(f"未获取到基金 {fund_code} 的股票持仓数据。")

    data = pd.concat(frames, ignore_index=True)

    required_cols = ["股票代码", "股票名称", "占净值比例", "季度"]
    missing = [c for c in required_cols if c not in data.columns]

    if missing:
        raise RuntimeError(f"股票持仓缺少必要字段: {missing}; 当前字段: {list(data.columns)}")

    data["占净值比例"] = pd.to_numeric(data["占净值比例"], errors="coerce")
    data["_quarter_key"] = data["季度"].apply(quarter_key)

    data = data.dropna(subset=["占净值比例"])
    data = data[data["_quarter_key"] >= 0]

    if data.empty:
        raise RuntimeError(f"基金 {fund_code} 股票持仓清洗后为空。")

    latest_key = data["_quarter_key"].max()
    latest_df = data[data["_quarter_key"] == latest_key].copy()

    latest_df = latest_df.sort_values("占净值比例", ascending=False).head(top_n)
    latest_df = latest_df.reset_index(drop=True)

    latest_df[["市场", "ticker"]] = latest_df.apply(
        lambda row: pd.Series(
            detect_market_and_ticker(
                row["股票代码"],
                row["股票名称"],
            )
        ),
        axis=1,
    )

    total_weight = latest_df["占净值比例"].sum()

    if total_weight <= 0:
        raise RuntimeError("前 N 大股票持仓权重合计无效。")

    latest_df["归一化权重"] = latest_df["占净值比例"] / total_weight * 100.0

    return latest_df



def _target_holding_quarter_key_for_now(now=None):
    """
    返回当前是否处于基金季报持仓披露试探窗口，以及本轮目标季度。

    窗口：
        Q4：1月20日 - 2月10日，目标上一年Q4
        Q1：4月20日 - 5月10日，目标当年Q1
        Q2：7月20日 - 8月10日，目标当年Q2
        Q3：10月20日 - 11月10日，目标当年Q3

    返回：
        target_key, window_end
        不在窗口时 target_key=None。
    """
    if now is None:
        now = datetime.now()

    y, m, d = now.year, now.month, now.day

    if m == 1 and d >= 20:
        return (y - 1) * 10 + 4, datetime(y, 2, 10, 23, 59, 59)
    if m == 2 and d <= 10:
        return (y - 1) * 10 + 4, datetime(y, 2, 10, 23, 59, 59)

    if m == 4 and d >= 20:
        return y * 10 + 1, datetime(y, 5, 10, 23, 59, 59)
    if m == 5 and d <= 10:
        return y * 10 + 1, datetime(y, 5, 10, 23, 59, 59)

    if m == 7 and d >= 20:
        return y * 10 + 2, datetime(y, 8, 10, 23, 59, 59)
    if m == 8 and d <= 10:
        return y * 10 + 2, datetime(y, 8, 10, 23, 59, 59)

    if m == 10 and d >= 20:
        return y * 10 + 3, datetime(y, 11, 10, 23, 59, 59)
    if m == 11 and d <= 10:
        return y * 10 + 3, datetime(y, 11, 10, 23, 59, 59)

    return None, None


def _next_holding_disclosure_window_start(now=None):
    """
    返回下一次基金持仓披露试探窗口开始时间。
    """
    if now is None:
        now = datetime.now()

    y = now.year
    candidates = [
        datetime(y, 1, 20),
        datetime(y, 4, 20),
        datetime(y, 7, 20),
        datetime(y, 10, 20),
        datetime(y + 1, 1, 20),
    ]
    for dt in candidates:
        if dt > now:
            return dt
    return datetime(y + 1, 1, 20)


def _holding_cache_item_to_df(item):
    """
    尝试从基金持仓缓存项恢复 DataFrame。
    """
    if not isinstance(item, dict) or not item.get("data_json"):
        return None
    try:
        return _df_from_cache_json(item["data_json"])
    except Exception as e:
        print(f"[WARN] 基金持仓缓存损坏，将重新获取: {e}", flush=True)
        return None


def _holding_df_quarter_meta(df):
    """
    从持仓 DataFrame 中提取最新季度 key 和 label。
    """
    if df is None or df.empty:
        return None, None

    qkey = None
    qlabel = None

    if "_quarter_key" in df.columns:
        try:
            vals = pd.to_numeric(df["_quarter_key"], errors="coerce").dropna()
            if not vals.empty:
                qkey = int(vals.iloc[0])
        except Exception:
            qkey = None

    if "季度" in df.columns and not df["季度"].empty:
        try:
            qlabel = str(df["季度"].iloc[0])
            if qkey is None:
                qkey = quarter_key(qlabel)
        except Exception:
            pass

    return qkey, qlabel


def get_latest_stock_holdings_df(
    fund_code="017437",
    top_n=10,
    holding_cache_days=None,
    cache_enabled=True,
):
    """
    获取基金最新披露季度前 N 大股票持仓，带文件缓存。

    当前策略：
        - 平时有缓存就直接用缓存，不主动刷新基金持仓；
        - 只在 1/4/7/10 月20日至次月10日的季报披露窗口低频试探；
        - 窗口内每只基金约每 3 天最多请求一次；
        - 某基金已拿到本轮目标季度后，停止请求，等下一季度窗口；
        - 请求失败或返回旧季度时保留旧缓存，不污染数据。

    holding_cache_days 仅保留用于兼容旧调用；新策略不依赖固定 75 天周期。
    """
    fund_code = str(fund_code).zfill(6)
    top_n = int(top_n)
    cache_key = f"{fund_code}:top{top_n}"

    if not cache_enabled:
        return get_latest_stock_holdings_df_uncached(
            fund_code=fund_code,
            top_n=top_n,
        )

    now = datetime.now()
    target_key, window_end = _target_holding_quarter_key_for_now(now)
    in_window = target_key is not None

    cache = _load_json_cache(FUND_HOLDINGS_CACHE_FILE, default={})
    item = cache.get(cache_key)
    cached_df = _holding_cache_item_to_df(item)

    # 无缓存：必须抓一次，否则无法估算。
    if cached_df is None:
        try:
            _cache_log(f"无基金持仓缓存，首次获取: {cache_key}")
            df = get_latest_stock_holdings_df_uncached(fund_code=fund_code, top_n=top_n)
            latest_key, latest_label = _holding_df_quarter_meta(df)
            confirmed = bool(target_key is not None and latest_key is not None and latest_key >= target_key)
            next_check = _next_holding_disclosure_window_start(now) if confirmed or not in_window else now + timedelta(days=3)

            cache[cache_key] = {
                "fetched_at": now.isoformat(timespec="seconds"),
                "last_checked_at": now.isoformat(timespec="seconds"),
                "next_check_after": next_check.isoformat(timespec="seconds"),
                "fund_code": fund_code,
                "top_n": top_n,
                "latest_quarter_label": latest_label,
                "latest_quarter_key": latest_key,
                "target_quarter_key": target_key,
                "target_quarter_confirmed": confirmed,
                "data_json": _df_to_cache_json(df),
            }
            _save_json_cache(FUND_HOLDINGS_CACHE_FILE, cache)
            return df
        except Exception:
            raise

    cached_key = None
    cached_label = None
    if isinstance(item, dict):
        cached_key = item.get("latest_quarter_key")
        cached_label = item.get("latest_quarter_label")

    if cached_key is None or cached_label is None:
        cached_key, cached_label = _holding_df_quarter_meta(cached_df)

    # 不在披露窗口：直接用缓存，不做无意义请求。
    if not in_window:
        next_window = _next_holding_disclosure_window_start(now)
        if isinstance(item, dict):
            item.update({
                "latest_quarter_label": cached_label,
                "latest_quarter_key": cached_key,
                "target_quarter_key": item.get("target_quarter_key"),
                "target_quarter_confirmed": bool(item.get("target_quarter_confirmed", False)),
                "next_check_after": item.get("next_check_after") or next_window.isoformat(timespec="seconds"),
            })
            cache[cache_key] = item
            _save_json_cache(FUND_HOLDINGS_CACHE_FILE, cache)

        _cache_log(f"非披露窗口，使用基金持仓缓存: {cache_key}")
        return cached_df

    # 已经拿到本轮目标季度：不再请求。
    if cached_key is not None and int(cached_key) >= int(target_key):
        next_window = _next_holding_disclosure_window_start(window_end or now)
        if isinstance(item, dict):
            item.update({
                "latest_quarter_label": cached_label,
                "latest_quarter_key": int(cached_key),
                "target_quarter_key": int(target_key),
                "target_quarter_confirmed": True,
                "next_check_after": next_window.isoformat(timespec="seconds"),
            })
            cache[cache_key] = item
            _save_json_cache(FUND_HOLDINGS_CACHE_FILE, cache)

        _cache_log(f"已确认目标季度持仓，使用缓存: {cache_key} -> {cached_label}")
        return cached_df

    # 尚未拿到目标季度：检查 next_check_after，未到时间则不请求。
    next_check_after = item.get("next_check_after") if isinstance(item, dict) else None
    if next_check_after:
        try:
            next_check_dt = pd.to_datetime(next_check_after).to_pydatetime()
            if now < next_check_dt:
                _cache_log(f"未到下次持仓检查时间，使用缓存: {cache_key}, next={next_check_after}")
                return cached_df
        except Exception:
            pass

    # 到达检查时间：低频试探。
    try:
        _cache_log(f"披露窗口内试探更新基金持仓: {cache_key}, target={target_key}")
        df = get_latest_stock_holdings_df_uncached(fund_code=fund_code, top_n=top_n)
        latest_key, latest_label = _holding_df_quarter_meta(df)

        # 防止接口返回更旧数据覆盖较新缓存。
        if cached_key is not None and latest_key is not None and int(latest_key) < int(cached_key):
            print(
                f"[WARN] 远程持仓季度旧于缓存，拒绝覆盖: {cache_key}, remote={latest_key}, cache={cached_key}",
                flush=True,
            )
            next_check = now + timedelta(days=3)
            item.update({
                "last_checked_at": now.isoformat(timespec="seconds"),
                "next_check_after": next_check.isoformat(timespec="seconds"),
                "target_quarter_key": int(target_key),
                "target_quarter_confirmed": False,
            })
            cache[cache_key] = item
            _save_json_cache(FUND_HOLDINGS_CACHE_FILE, cache)
            return cached_df

        confirmed = bool(latest_key is not None and int(latest_key) >= int(target_key))
        next_check = _next_holding_disclosure_window_start(window_end or now) if confirmed else now + timedelta(days=3)

        cache[cache_key] = {
            "fetched_at": now.isoformat(timespec="seconds"),
            "last_checked_at": now.isoformat(timespec="seconds"),
            "next_check_after": next_check.isoformat(timespec="seconds"),
            "fund_code": fund_code,
            "top_n": top_n,
            "latest_quarter_label": latest_label,
            "latest_quarter_key": latest_key,
            "target_quarter_key": int(target_key),
            "target_quarter_confirmed": confirmed,
            "data_json": _df_to_cache_json(df),
        }
        _save_json_cache(FUND_HOLDINGS_CACHE_FILE, cache)

        if confirmed:
            _cache_log(f"已更新到目标季度持仓: {cache_key} -> {latest_label}")
        else:
            _cache_log(f"远程仍未披露目标季度，保留本次最新持仓: {cache_key} -> {latest_label}")

        return df

    except Exception as e:
        print(f"[WARN] 基金持仓更新失败，使用旧缓存: {cache_key}, 原因: {e}", flush=True)
        if isinstance(item, dict):
            next_check = now + timedelta(days=3)
            item.update({
                "last_checked_at": now.isoformat(timespec="seconds"),
                "next_check_after": next_check.isoformat(timespec="seconds"),
                "target_quarter_key": int(target_key),
                "target_quarter_confirmed": False,
            })
            cache[cache_key] = item
            _save_json_cache(FUND_HOLDINGS_CACHE_FILE, cache)

        return cached_df



def _append_detail_row_without_concat_warning(df: pd.DataFrame, row: dict) -> pd.DataFrame:
    """
    向 detail DataFrame 追加一行，避免 pandas 在 concat / loc 追加全 NA 列时触发 FutureWarning。

    说明：
        - 不使用 pd.concat([df, one_row_df])；
        - 不使用 df.loc[len(df)] = ... 直接扩展；
        - 先 reindex 扩展一个唯一索引，再逐列写入。
    """
    out = df.copy()

    for col in row.keys():
        if col not in out.columns:
            out[col] = pd.NA

    new_index = "__extra_row__"
    suffix = 0
    while new_index in out.index:
        suffix += 1
        new_index = f"__extra_row__{suffix}"

    out = out.reindex(list(out.index) + [new_index])

    for col in out.columns:
        out.at[new_index, col] = row.get(col, pd.NA)

    return out.reset_index(drop=True)

def estimate_stock_holdings_return(
    latest_df,
    manual_returns_pct=None,
    prefer_intraday=True,
    us_realtime=False,
    hk_realtime=False,
    failed_return_as_zero=True,
    renormalize_available_holdings=True,
    security_return_cache_enabled=True,
    cn_hk_hourly_cache=True,
    valuation_mode="intraday",
    stock_residual_benchmark_return_pct=None,
    stock_residual_benchmark_label=None,
    stock_residual_benchmark_source=None,
):
    """
    使用前 N 大股票持仓估算基金收益。

    核心逻辑
    --------
    1. 先按披露的前 N 大持仓权重计算“归一化权重”；
    2. 获取每只持仓的当日涨跌幅；
    3. 如果某些持仓无法获取行情：
        - renormalize_available_holdings=True：
            剔除失败持仓，并把“可获取行情的持仓”再次归一化到 100% 后计算；
            适合你说的“日东纺这类无法获取数据时，用能查到的持仓股估算”。
        - renormalize_available_holdings=False：
            不重新归一化，缺失持仓贡献为空，等价于把缺失仓位视为未估算。
    """
    df = latest_df.copy()

    requested_valuation_mode = _normalize_valuation_mode(valuation_mode)
    if requested_valuation_mode == "auto":
        effective_valuation_mode = _resolve_auto_valuation_mode_from_markets(df.get("市场", []))
    else:
        effective_valuation_mode = requested_valuation_mode

    returns = []
    sources = []
    warnings = []

    for _, row in df.iterrows():
        market = row["市场"]
        ticker = row["ticker"]
        name = row["股票名称"]

        try:
            r_pct, source = get_stock_return_pct(
                market=market,
                ticker=ticker,
                manual_returns_pct=manual_returns_pct,
                prefer_intraday=prefer_intraday,
                us_realtime=us_realtime,
                hk_realtime=hk_realtime,
                security_return_cache_enabled=security_return_cache_enabled,
                cn_hk_hourly_cache=cn_hk_hourly_cache,
                valuation_mode=effective_valuation_mode,
            )
        except Exception as e:
            if failed_return_as_zero:
                r_pct, source = None, "failed"
                warnings.append(f"{name}({ticker}) 涨跌幅获取失败，已从有效估算权重中剔除：{e}")
            else:
                raise

        returns.append(r_pct)
        sources.append(source)

    df["当日涨跌幅"] = returns
    df["收益数据源"] = sources

    valid_mask = df["当日涨跌幅"].notna()
    valid_count = int(valid_mask.sum())
    failed_count = int((~valid_mask).sum())

    df["有效估算权重"] = pd.NA
    df["收益贡献"] = pd.NA

    # 海外股票持仓型基金的增强补偿口径：
    # 行情有效持仓按原始占净值比例放大；行情失败持仓与未披露仓位进入纳斯达克100补偿仓位。
    raw_weight_sum_pct = float(pd.to_numeric(df["占净值比例"], errors="coerce").fillna(0).sum())
    available_raw_weight_sum_pct = float(pd.to_numeric(df.loc[valid_mask, "占净值比例"], errors="coerce").fillna(0).sum())
    failed_raw_weight_sum_pct = float(pd.to_numeric(df.loc[~valid_mask, "占净值比例"], errors="coerce").fillna(0).sum())

    use_residual_benchmark = stock_residual_benchmark_return_pct is not None

    if use_residual_benchmark:
        residual_label = stock_residual_benchmark_label or "剩余仓位基准"
        residual_source = stock_residual_benchmark_source or "residual_benchmark"
        residual_return_pct = float(stock_residual_benchmark_return_pct)

        # 海外股票持仓型基金专用口径：
        # 1. 行情有效的已披露持仓不再只按原始占净值比例计算，而是乘以人工放大系数；
        # 2. 行情失败的已披露持仓划入纳斯达克100补偿仓位；
        # 3. 未披露仓位也划入纳斯达克100补偿仓位；
        # 4. 为避免总权重超过 100%，补偿仓位 = 100% - 放大后的有效持仓权重。
        try:
            holding_boost = float(OVERSEAS_VALID_HOLDING_BOOST)
        except Exception:
            holding_boost = 1.0

        if not pd.notna(holding_boost) or holding_boost < 0:
            holding_boost = 1.0

        uncapped_boosted_available_weight_sum_pct = available_raw_weight_sum_pct * holding_boost

        if available_raw_weight_sum_pct > 0:
            # 封顶保护：无论 OVERSEAS_VALID_HOLDING_BOOST 设多大，
            # 放大后的有效持仓估算权重最多为 100%，避免总估算权重超过 100%。
            boosted_available_weight_sum_pct = min(
                100.0,
                uncapped_boosted_available_weight_sum_pct,
            )
            actual_boost = boosted_available_weight_sum_pct / available_raw_weight_sum_pct
        else:
            boosted_available_weight_sum_pct = 0.0
            actual_boost = 0.0

        cap_applied = uncapped_boosted_available_weight_sum_pct > 100.0
        residual_weight_pct = max(0.0, 100.0 - boosted_available_weight_sum_pct)

        if valid_count == 0 and residual_weight_pct <= 0:
            estimated_return_pct = None
            available_weight_sum_pct = 0.0
            failed_weight_sum_pct = failed_raw_weight_sum_pct
            method = "stock_boosted_raw_plus_residual_benchmark_failed"
        else:
            # 行情有效的已披露持仓：原始占净值比例 × 实际放大系数。
            if valid_count > 0:
                df.loc[valid_mask, "有效估算权重"] = (
                    df.loc[valid_mask, "占净值比例"] * actual_boost
                )
                df.loc[valid_mask, "收益贡献"] = (
                    df.loc[valid_mask, "有效估算权重"] * df.loc[valid_mask, "当日涨跌幅"] / 100.0
                )

            known_contribution = float(pd.to_numeric(df.loc[valid_mask, "收益贡献"], errors="coerce").sum())
            residual_contribution = residual_weight_pct * residual_return_pct / 100.0
            estimated_return_pct = known_contribution + residual_contribution

            available_weight_sum_pct = boosted_available_weight_sum_pct
            failed_weight_sum_pct = failed_raw_weight_sum_pct
            method = "stock_boosted_raw_plus_residual_benchmark"

            if residual_weight_pct > 0:
                residual_row = {
                    "股票代码": "RESIDUAL",
                    "股票名称": f"补偿仓位（{residual_label}）",
                    "占净值比例": residual_weight_pct,
                    "季度": "失败持仓与未披露仓位基准补偿",
                    "_quarter_key": pd.NA,
                    "市场": "INDEX",
                    "ticker": residual_label,
                    "归一化权重": pd.NA,
                    "当日涨跌幅": residual_return_pct,
                    "收益数据源": residual_source,
                    "有效估算权重": residual_weight_pct,
                    "收益贡献": residual_contribution,
                }
                df = _append_detail_row_without_concat_warning(df, residual_row)

            unreported_weight_pct = max(0.0, 100.0 - raw_weight_sum_pct)
            transferred_boost_weight_pct = max(0.0, boosted_available_weight_sum_pct - available_raw_weight_sum_pct)

            if cap_applied:
                warnings.append(
                    f"有效持仓放大后超过 100%，已执行封顶保护："
                    f"原始有效持仓 {available_raw_weight_sum_pct:.2f}% × 配置放大系数 {holding_boost:.2f} "
                    f"= {uncapped_boosted_available_weight_sum_pct:.2f}%，"
                    f"实际有效估算权重封顶为 {boosted_available_weight_sum_pct:.2f}%，"
                    f"实际放大系数 {actual_boost:.4f}。"
                )

            warnings.append(
                f"已启用海外股票持仓增强补偿口径：已披露前N大持仓合计 {raw_weight_sum_pct:.2f}%，"
                f"其中行情有效 {available_raw_weight_sum_pct:.2f}%，行情失败 {failed_raw_weight_sum_pct:.2f}%，"
                f"未披露仓位 {unreported_weight_pct:.2f}%；"
                f"有效持仓放大系数 {holding_boost:.2f}，实际有效估算权重 {boosted_available_weight_sum_pct:.2f}%，"
                f"从基准补偿仓位转移 {transferred_boost_weight_pct:.2f}% 给有效持仓；"
                f"补偿仓位 {residual_weight_pct:.2f}% 使用 {residual_label} {residual_return_pct:+.4f}% 估算。"
            )
    else:
        if valid_count == 0:
            estimated_return_pct = None
            available_weight_sum_pct = 0.0
            failed_weight_sum_pct = float(df["归一化权重"].sum())
            method = "stock_topn_available_normalized_failed"
        else:
            available_weight_sum_pct = float(df.loc[valid_mask, "归一化权重"].sum())
            failed_weight_sum_pct = float(df.loc[~valid_mask, "归一化权重"].sum())

            if available_weight_sum_pct <= 0:
                estimated_return_pct = None
                method = "stock_topn_available_normalized_failed"
            else:
                if renormalize_available_holdings:
                    df.loc[valid_mask, "有效估算权重"] = (
                        df.loc[valid_mask, "归一化权重"] / available_weight_sum_pct * 100.0
                    )
                    method = "stock_topn_available_normalized"
                else:
                    df.loc[valid_mask, "有效估算权重"] = df.loc[valid_mask, "归一化权重"]
                    method = "stock_topn_original_normalized"

                df.loc[valid_mask, "收益贡献"] = (
                    df.loc[valid_mask, "有效估算权重"] * df.loc[valid_mask, "当日涨跌幅"] / 100.0
                )
                estimated_return_pct = float(pd.to_numeric(df.loc[valid_mask, "收益贡献"], errors="coerce").sum())

    summary = {
        "method": method,
        "raw_weight_sum_pct": raw_weight_sum_pct,
        "normalized_weight_sum_pct": float(pd.to_numeric(df.get("归一化权重", pd.Series(dtype=float)), errors="coerce").sum()),
        "available_normalized_weight_sum_pct": float(available_weight_sum_pct),
        "failed_normalized_weight_sum_pct": float(failed_weight_sum_pct),
        "available_raw_weight_sum_pct": available_raw_weight_sum_pct,
        "failed_raw_weight_sum_pct": failed_raw_weight_sum_pct,
        "residual_benchmark_enabled": bool(use_residual_benchmark),
        "residual_benchmark_label": stock_residual_benchmark_label,
        "residual_benchmark_return_pct": None if stock_residual_benchmark_return_pct is None else float(stock_residual_benchmark_return_pct),
        "residual_benchmark_weight_pct": float(residual_weight_pct) if use_residual_benchmark and "residual_weight_pct" in locals() else 0.0,
        "overseas_valid_holding_boost": float(OVERSEAS_VALID_HOLDING_BOOST) if use_residual_benchmark else 1.0,
        "boosted_available_raw_weight_sum_pct": float(available_weight_sum_pct) if use_residual_benchmark else available_raw_weight_sum_pct,
        "valid_holding_count": valid_count,
        "failed_holding_count": failed_count,
        "renormalize_available_holdings": bool(renormalize_available_holdings),
        "requested_valuation_mode": requested_valuation_mode,
        "effective_valuation_mode": effective_valuation_mode,
        "estimated_return_pct": estimated_return_pct,
        "warnings": warnings,
    }

    return df, summary


# ============================================================
# 6. ETF 联接 / FOF / 指数代理估算
# ============================================================

def estimate_proxy_components_return(
    fund_code,
    proxy_map=None,
    manual_returns_pct=None,
    prefer_intraday=True,
    us_realtime=False,
    hk_realtime=False,
    proxy_normalize_weights=False,
    failed_return_as_zero=True,
    security_return_cache_enabled=True,
    cn_hk_hourly_cache=True,
    valuation_mode="intraday",
):
    """
    根据 proxy_map 中的底层 ETF / 指数组件估算基金涨跌幅。

    proxy_normalize_weights:
        False：
            默认。按组件原始 weight_pct 计算，现金仓位视为 0。
            适合 ETF 联接基金。
        True：
            将所有组件权重归一化到 100% 后估算。
            适合只想看代理资产本身表现。
    """
    fund_code = str(fund_code).zfill(6)

    if proxy_map is None:
        proxy_map = DEFAULT_FUND_PROXY_MAP

    if fund_code not in proxy_map:
        raise RuntimeError(
            f"基金 {fund_code} 未配置代理资产。请在 DEFAULT_FUND_PROXY_MAP 或 proxy_map 中增加配置。"
        )

    config = proxy_map[fund_code]
    components = config.get("components", [])

    requested_valuation_mode = _normalize_valuation_mode(valuation_mode)
    if requested_valuation_mode == "auto":
        effective_valuation_mode = _resolve_auto_valuation_mode_from_components(components)
    else:
        effective_valuation_mode = requested_valuation_mode

    if not components:
        raise RuntimeError(f"基金 {fund_code} 的代理配置缺少 components。")

    df = pd.DataFrame(components).copy()

    if "weight_pct" not in df.columns:
        raise RuntimeError(f"基金 {fund_code} 的代理组件缺少 weight_pct。")

    df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce")
    df = df.dropna(subset=["weight_pct"])

    if df.empty or df["weight_pct"].sum() <= 0:
        raise RuntimeError(f"基金 {fund_code} 的代理组件权重无效。")

    if proxy_normalize_weights:
        df["估算权重"] = df["weight_pct"] / df["weight_pct"].sum() * 100.0
        weight_mode = "normalized_proxy_weights"
    else:
        df["估算权重"] = df["weight_pct"]
        weight_mode = "raw_proxy_weights_cash_as_zero"

    returns = []
    sources = []
    warnings = []

    for _, row in df.iterrows():
        component = row.to_dict()
        name = component.get("name", component.get("code", ""))

        try:
            r_pct, source = get_proxy_return_pct(
                component=component,
                manual_returns_pct=manual_returns_pct,
                prefer_intraday=prefer_intraday,
                us_realtime=us_realtime,
                hk_realtime=hk_realtime,
                security_return_cache_enabled=security_return_cache_enabled,
                cn_hk_hourly_cache=cn_hk_hourly_cache,
                valuation_mode=effective_valuation_mode,
            )
        except Exception as e:
            if failed_return_as_zero:
                r_pct, source = None, "failed"
                warnings.append(f"{name} 代理涨跌幅获取失败：{e}")
            else:
                raise

        returns.append(r_pct)
        sources.append(source)

    df["当日涨跌幅"] = returns
    df["收益数据源"] = sources

    valid_df = df.dropna(subset=["当日涨跌幅"]).copy()

    if valid_df.empty:
        estimated_return_pct = None
        df["收益贡献"] = None
    else:
        df["收益贡献"] = df["估算权重"] * df["当日涨跌幅"] / 100.0
        estimated_return_pct = float(df["收益贡献"].sum(skipna=True))

    summary = {
        "method": "proxy_components",
        "weight_mode": weight_mode,
        "proxy_description": config.get("description", ""),
        "raw_weight_sum_pct": float(df["weight_pct"].sum()),
        "estimated_weight_sum_pct": float(df["估算权重"].sum()),
        "estimated_return_pct": estimated_return_pct,
        "requested_valuation_mode": requested_valuation_mode,
        "effective_valuation_mode": effective_valuation_mode,
        "warnings": warnings,
    }

    return df, summary


# ============================================================
# 7. 单基金估算与批量估算
# ============================================================

def estimate_one_fund(
    fund_code,
    top_n=10,
    manual_returns_pct=None,
    prefer_intraday=True,
    us_realtime=False,
    hk_realtime=False,
    renormalize_available_holdings=True,
    include_purchase_limit=True,
    purchase_limit_timeout=8,
    purchase_limit_cache_days=7,
    holding_cache_days=75,
    cache_enabled=True,
    security_return_cache_enabled=True,
    cn_hk_hourly_cache=True,
    holding_mode="auto",
    proxy_map=None,
    proxy_normalize_weights=False,
    valuation_mode="intraday",
    stock_residual_benchmark_return_pct=None,
    stock_residual_benchmark_label=None,
    stock_residual_benchmark_source=None,
):
    """
    估算单只基金的今日涨跌幅。

    参数
    ----
    fund_code:
        基金代码。

    top_n:
        股票型基金取前 N 大股票持仓。

    manual_returns_pct:
        手动覆盖涨跌幅，单位百分数。
        示例：{"NVDA": 4.00, "512890": 0.35, "^GSPC": -0.20}

    prefer_intraday:
        为兼容旧调用保留；当前版本不再使用 已移除的外部指数直连 接口。

    us_realtime:
        是否启用美股实时行情。

    renormalize_available_holdings:
        True：
            如果部分持仓行情获取失败，则只使用可获取行情的持仓，并把这些持仓重新归一化到 100%。
        False：
            不重新归一化，缺失持仓不参与贡献。

    include_purchase_limit:
        是否获取限购金额。

    purchase_limit_timeout:
        限购网页请求超时秒数。

    holding_mode:
        "auto"：
            如果 fund_code 在 proxy_map 中，优先走代理估算；
            否则走股票持仓估算。
        "stock"：
            强制股票持仓估算。
        "proxy"：
            强制代理估算，适合 ETF 联接 / FOF。

    proxy_map:
        代理映射表。None 时使用 DEFAULT_FUND_PROXY_MAP。

    proxy_normalize_weights:
        ETF/FOF 代理组件是否归一化到 100%。
        False：默认，按原始权重计算，现金按 0。
        True ：代理组件归一化到 100%。
    """
    fund_code = str(fund_code).zfill(6)
    fund_name = get_fund_name(fund_code)

    if proxy_map is None:
        proxy_map = DEFAULT_FUND_PROXY_MAP

    mode = str(holding_mode).strip().lower()

    if mode not in {"auto", "stock", "proxy"}:
        raise ValueError("holding_mode 只能是 'auto', 'stock', 'proxy'")

    detail_df = None
    summary = None

    if mode == "proxy" or (mode == "auto" and fund_code in proxy_map):
        detail_df, summary = estimate_proxy_components_return(
            fund_code=fund_code,
            proxy_map=proxy_map,
            manual_returns_pct=manual_returns_pct,
            prefer_intraday=prefer_intraday,
            us_realtime=us_realtime,
            hk_realtime=hk_realtime,
            proxy_normalize_weights=proxy_normalize_weights,
            security_return_cache_enabled=security_return_cache_enabled,
            cn_hk_hourly_cache=cn_hk_hourly_cache,
            valuation_mode=valuation_mode,
        )
    else:
        latest_df = get_latest_stock_holdings_df(
            fund_code=fund_code,
            top_n=top_n,
            holding_cache_days=holding_cache_days,
            cache_enabled=cache_enabled,
        )

        detail_df, summary = estimate_stock_holdings_return(
            latest_df=latest_df,
            manual_returns_pct=manual_returns_pct,
            prefer_intraday=prefer_intraday,
            us_realtime=us_realtime,
            hk_realtime=hk_realtime,
            renormalize_available_holdings=renormalize_available_holdings,
            security_return_cache_enabled=security_return_cache_enabled,
            cn_hk_hourly_cache=cn_hk_hourly_cache,
            valuation_mode=valuation_mode,
            stock_residual_benchmark_return_pct=stock_residual_benchmark_return_pct,
            stock_residual_benchmark_label=stock_residual_benchmark_label,
            stock_residual_benchmark_source=stock_residual_benchmark_source,
        )

    summary["valuation_mode"] = summary.get("effective_valuation_mode", _normalize_valuation_mode(valuation_mode))

    result_row = {
        "基金代码": fund_code,
        "基金名称": fund_name,
        "今日预估涨跌幅": summary["estimated_return_pct"],
        "_估算方式": summary.get("method", ""),
    }

    if include_purchase_limit:
        result_row["限购金额"] = get_fund_purchase_limit(
            fund_code=fund_code,
            timeout=purchase_limit_timeout,
            cache_days=purchase_limit_cache_days,
            cache_enabled=cache_enabled,
        )

    return result_row, detail_df, summary


def estimate_funds(
    fund_codes,
    top_n=10,
    manual_returns_pct=None,
    prefer_intraday=True,
    us_realtime=False,
    hk_realtime=False,
    renormalize_available_holdings=True,
    include_purchase_limit=True,
    purchase_limit_timeout=8,
    purchase_limit_cache_days=7,
    holding_cache_days=75,
    cache_enabled=True,
    security_return_cache_enabled=True,
    cn_hk_hourly_cache=True,
    sort_by_return=True,
    holding_mode="auto",
    proxy_map=None,
    proxy_normalize_weights=False,
    include_method_col=False,
    valuation_mode="intraday",
    stock_residual_benchmark_return_pct=None,
    stock_residual_benchmark_label=None,
    stock_residual_benchmark_source=None,
):
    """
    批量估算多只基金的今日预估涨跌幅。

    参数
    ----
    sort_by_return:
        True：按“今日预估涨跌幅”从高到低排序，并重新编号。
        False：保留 fund_codes 输入顺序。

    holding_mode:
        "auto"：自动选择股票持仓或代理估算。
        "stock"：强制股票持仓。
        "proxy"：强制代理估算。

    include_method_col:
        True：表格显示“估算方式”列。
        False：不显示，保持表格简洁。

    renormalize_available_holdings:
        True：部分股票行情失败时，把剩余可查持仓重新归一化到 100% 后估算。
        False：失败持仓不参与收益贡献，但不重新分配其权重。

    返回
    ----
    result_df, detail_map
    """
    if isinstance(fund_codes, str):
        fund_codes = [fund_codes]

    if proxy_map is None:
        proxy_map = DEFAULT_FUND_PROXY_MAP

    rows = []
    detail_map = {}

    for i, fund_code in enumerate(fund_codes, start=1):
        code = str(fund_code).zfill(6)

        try:
            result_row, detail_df, summary = estimate_one_fund(
                fund_code=code,
                top_n=top_n,
                manual_returns_pct=manual_returns_pct,
                prefer_intraday=prefer_intraday,
                us_realtime=us_realtime,
                hk_realtime=hk_realtime,
                renormalize_available_holdings=renormalize_available_holdings,
                include_purchase_limit=include_purchase_limit,
                purchase_limit_timeout=purchase_limit_timeout,
                purchase_limit_cache_days=purchase_limit_cache_days,
                holding_cache_days=holding_cache_days,
                cache_enabled=cache_enabled,
                security_return_cache_enabled=security_return_cache_enabled,
                cn_hk_hourly_cache=cn_hk_hourly_cache,
                holding_mode=holding_mode,
                proxy_map=proxy_map,
                proxy_normalize_weights=proxy_normalize_weights,
                valuation_mode=valuation_mode,
                stock_residual_benchmark_return_pct=stock_residual_benchmark_return_pct,
                stock_residual_benchmark_label=stock_residual_benchmark_label,
                stock_residual_benchmark_source=stock_residual_benchmark_source,
            )

            result_row["_输入顺序"] = i
            rows.append(result_row)

            detail_map[code] = {
                "detail_df": detail_df,
                "summary": summary,
                "error": None,
            }

        except Exception as e:
            error_row = {
                "_输入顺序": i,
                "基金代码": code,
                "基金名称": get_fund_name(code),
                "今日预估涨跌幅": None,
                "_估算方式": "failed",
            }

            if include_purchase_limit:
                error_row["限购金额"] = get_fund_purchase_limit(
                    fund_code=code,
                    timeout=purchase_limit_timeout,
                    cache_days=purchase_limit_cache_days,
                    cache_enabled=cache_enabled,
                )

            rows.append(error_row)

            detail_map[code] = {
                "detail_df": None,
                "summary": None,
                "error": repr(e),
            }

            print(f"[WARN] 基金 {code} 估算失败: {e}")

    result_df = pd.DataFrame(rows)

    if sort_by_return:
        result_df["_排序收益"] = pd.to_numeric(
            result_df["今日预估涨跌幅"],
            errors="coerce",
        )

        result_df = result_df.sort_values(
            by=["_排序收益", "_输入顺序"],
            ascending=[False, True],
            na_position="last",
        ).reset_index(drop=True)

        result_df = result_df.drop(columns=["_排序收益"])
    else:
        result_df = result_df.sort_values("_输入顺序").reset_index(drop=True)

    result_df["序号"] = range(1, len(result_df) + 1)

    cols = ["序号", "基金代码", "基金名称", "今日预估涨跌幅"]

    if include_purchase_limit:
        cols.append("限购金额")

    if include_method_col:
        result_df["估算方式"] = result_df["_估算方式"]
        cols.append("估算方式")

    result_df = result_df[cols]

    return result_df, detail_map


# ============================================================
# 7.5 市场基准：直接获取指数涨跌幅
# ============================================================


def fetch_us_index_return_pct_from_rsi_module(symbol, display_name=None, days=15):
    """
    使用 tools/rsi_module.py 中已经验证过的指数行情入口获取美股指数最新完整交易日涨跌幅。

    约定：
        .NDX -> 纳斯达克100
        .INX -> 标普500

    返回：
        return_pct, trade_date, source
    """
    symbol = str(symbol).strip()
    display_name = display_name or symbol

    last_error = None

    import_candidates = [
        ("tools.rsi_module", "get_us_index_akshare"),
        ("tools.rsi_modul", "get_us_index_akshare"),
        ("rsi_module", "get_us_index_akshare"),
        ("rsi_modul", "get_us_index_akshare"),
    ]

    getter = None
    for module_name, func_name in import_candidates:
        try:
            module = __import__(module_name, fromlist=[func_name])
            getter = getattr(module, func_name)
            break
        except Exception as e:
            last_error = e

    if getter is None:
        raise RuntimeError(f"无法导入 rsi_module.get_us_index_akshare: {last_error}")

    try:
        df = getter(
            symbol=symbol,
            days=days,
            cache_dir="cache",
            retry=3,
            use_cache=True,
            allow_eastmoney=False,
            include_realtime=False,
        )
    except TypeError:
        # 兼容旧函数签名
        df = getter(symbol=symbol, days=days)

    if df is None or df.empty:
        raise RuntimeError(f"rsi_module 返回空数据: {display_name}({symbol})")

    out = df.copy()

    rename_map = {
        "日期": "date",
        "收盘": "close",
        "Date": "date",
        "Close": "close",
    }
    out = out.rename(columns=rename_map)

    if "date" not in out.columns or "close" not in out.columns:
        raise RuntimeError(
            f"rsi_module 指数数据缺少 date 或 close 列: {display_name}({symbol}), columns={list(out.columns)}"
        )

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["close"] = pd.to_numeric(
        out["close"].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    )
    out = out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)

    if len(out) < 2:
        raise RuntimeError(f"rsi_module 有效收盘点不足: {display_name}({symbol})")

    last_close = float(out.iloc[-1]["close"])
    prev_close = float(out.iloc[-2]["close"])

    if prev_close == 0:
        raise RuntimeError(f"rsi_module 前一收盘价为0: {display_name}({symbol})")

    return_pct = (last_close / prev_close - 1.0) * 100.0
    trade_date = pd.Timestamp(out.iloc[-1]["date"]).strftime("%Y-%m-%d")

    return return_pct, trade_date, "rsi_module_index_daily"


def fetch_us_index_return_pct_yahoo(symbol, display_name=None, retry=2, sleep_seconds=0.8):
    """
    从 Yahoo Finance chart 接口直接获取美股指数最新完整交易日涨跌幅。

    这是备用兜底。常规情况下优先使用 tools/rsi_module.py 的 get_us_index_akshare。
    """
    symbol = str(symbol).strip().upper()
    display_name = display_name or symbol
    encoded_symbol = requests.utils.quote(symbol, safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_symbol}"
    params = {
        "range": "15d",
        "interval": "1d",
        "includePrePost": "false",
        "events": "history",
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Referer": "https://finance.yahoo.com/",
    }

    last_error = None

    for i in range(max(1, retry)):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=12)
            resp.raise_for_status()
            data = resp.json()
            result = data.get("chart", {}).get("result", [None])[0]

            if not result:
                raise RuntimeError(f"Yahoo 返回空 result: {display_name}({symbol})")

            timestamps = result.get("timestamp") or []
            quote = (result.get("indicators", {}).get("quote") or [{}])[0]
            closes = quote.get("close") or []

            points = []
            for ts, close in zip(timestamps, closes):
                if close is None:
                    continue
                try:
                    close_f = float(close)
                except Exception:
                    continue
                if close_f > 0:
                    points.append((int(ts), close_f))

            if len(points) < 2:
                raise RuntimeError(f"Yahoo 有效收盘点不足: {display_name}({symbol})")

            prev_ts, prev_close = points[-2]
            last_ts, last_close = points[-1]

            if prev_close == 0:
                raise RuntimeError(f"Yahoo 前一收盘价为0: {display_name}({symbol})")

            return_pct = (last_close / prev_close - 1.0) * 100.0
            trade_date = datetime.utcfromtimestamp(last_ts).strftime("%Y-%m-%d")
            return return_pct, trade_date, "yahoo_index_chart"

        except Exception as e:
            last_error = e
            if i < max(1, retry) - 1:
                time.sleep(sleep_seconds)

    raise RuntimeError(f"指数 {display_name}({symbol}) 获取失败: {last_error}")


def get_us_index_return_pct_cached(symbol, display_name=None, cache_enabled=True, cache_hours=36):
    """
    获取美股指数最新完整交易日涨跌幅，带 JSON 缓存。

    优先使用 tools/rsi_module.py 的稳定指数接口；
    失败后再尝试 Yahoo chart 兜底。
    """
    symbol = str(symbol).strip().upper()
    display_name = display_name or symbol
    today_bucket = datetime.now().strftime("%Y-%m-%d")
    cache_key = f"INDEX:{symbol}:{today_bucket}:last_close"

    if cache_enabled:
        cache = _load_json_cache(SECURITY_RETURN_CACHE_FILE, default={})
        item = cache.get(cache_key)
        if item and _is_cache_fresh(item.get("fetched_at"), max_age_hours=cache_hours):
            try:
                return (
                    float(item["return_pct"]),
                    str(item.get("trade_date", "")),
                    item.get("source", "file_cache"),
                )
            except Exception:
                pass

    _cache_log(f"重新获取指数: {display_name}({symbol}) [last_close]")

    errors = []

    try:
        r_pct, trade_date, source = fetch_us_index_return_pct_from_rsi_module(
            symbol=symbol,
            display_name=display_name,
        )
    except Exception as e:
        errors.append(f"rsi_module: {repr(e)}")
        try:
            # Yahoo 兜底使用常见 ^NDX / ^GSPC 写法
            yahoo_symbol = {
                ".NDX": "^NDX",
                ".INX": "^GSPC",
                ".IXIC": "^IXIC",
                ".DJI": "^DJI",
            }.get(symbol, symbol)
            r_pct, trade_date, source = fetch_us_index_return_pct_yahoo(
                symbol=yahoo_symbol,
                display_name=display_name,
            )
        except Exception as e2:
            errors.append(f"yahoo: {repr(e2)}")
            raise RuntimeError(f"指数 {display_name}({symbol}) 获取失败: {' | '.join(errors)}")

    if cache_enabled:
        cache = _load_json_cache(SECURITY_RETURN_CACHE_FILE, default={})
        cache[cache_key] = {
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "market": "INDEX",
            "ticker": symbol,
            "name": display_name,
            "return_pct": float(r_pct),
            "trade_date": trade_date,
            "source": source,
            "valuation_mode": "last_close",
        }
        _save_json_cache(SECURITY_RETURN_CACHE_FILE, cache)

    return r_pct, trade_date, source


def get_us_index_benchmark_items(cache_enabled=True):
    """
    获取海外表底部基准信息。直接获取指数涨跌幅，不使用 QQQ/SPY 代理。

    数据源优先使用 tools/rsi_module.py：
        .NDX -> 纳斯达克100
        .INX -> 标普500
    """
    specs = [
        {"label": "纳斯达克100", "symbol": ".NDX"},
        {"label": "标普500", "symbol": ".INX"},
    ]

    items = []
    for spec in specs:
        label = spec["label"]
        symbol = spec["symbol"]

        try:
            r_pct, trade_date, source = get_us_index_return_pct_cached(
                symbol=symbol,
                display_name=label,
                cache_enabled=cache_enabled,
            )
            items.append({
                "label": label,
                "symbol": symbol,
                "return_pct": r_pct,
                "trade_date": trade_date,
                "source": source,
                "error": None,
            })
        except Exception as e:
            print(f"[WARN] 指数基准 {label}({symbol}) 获取失败: {e}", flush=True)
            items.append({
                "label": label,
                "symbol": symbol,
                "return_pct": None,
                "trade_date": "",
                "source": "failed",
                "error": str(e),
            })

    return items


# ============================================================
# 8. 表格打印与图片输出
# ============================================================

def print_fund_estimate_table(result_df, title=None, pct_digits=4):
    """
    打印多基金估算结果。
    """
    if title is None:
        title = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    show_df = result_df.copy()
    show_df["今日预估涨跌幅"] = show_df["今日预估涨跌幅"].map(lambda x: format_pct(x, digits=pct_digits))

    print("=" * 100)
    print(title)
    print("=" * 100)
    print(show_df.to_string(index=False))
    print("=" * 100)


def save_fund_estimate_table_image(
    result_df,
    output_file="output/fund_estimate_table.png",
    title=None,
    dpi=220,
    watermark_text="鱼师",
    watermark_alpha=0.06,
    watermark_fontsize=32,
    watermark_rotation=28,
    watermark_rows=3,
    watermark_cols=2,
    watermark_color="#999999",
    watermark_zorder=3,
    up_color="red",
    down_color="green",
    neutral_color="black",
    pct_digits=4,
    header_bg="#2f3b52",
    header_text_color="white",
    grid_color="#d9d9d9",
    figure_width=None,
    row_height=0.45,
    footnote_text="按照披露的持仓股仓位或持仓仓位预估收益率",
    footnote_color="#666666",
    footnote_fontsize=15,
    benchmark_footer_items=None,
    benchmark_footer_fontsize=15,
    title_fontsize=20,
    title_color="black",
    title_fontweight="bold",
    title_gap_ratio=0.10, # 表格高度的百分比，作为标题与表格之间的间距
    title_gap_min=0.008, # 最小间距，避免表格较高时标题过远
    title_gap_max=0.026, # 最大间距，避免表格较矮时标题过近
    footnote_gap_ratio=0.10, # 表格高度的百分比，作为备注与表格之间的间距
    footnote_gap_min=0.008, # 最小间距，避免表格较高时备注过远
    footnote_gap_max=0.026, # 最大间距，避免表格较矮时备注过近
    pad_inches=0.12, # 图片周围的额外留白，单位英寸
):
    """
    保存基金预估收益表格图片。

    本版本对标题和备注做了自适应定位：
        1. 先绘制表格；
        2. 读取表格真实边界；
        3. 标题自动贴近表格上沿；
        4. 备注自动贴近表格下沿；
        5. 不再使用 ax.set_title() 和固定 fig.text(y=0.03) 位置。

    今日预估涨跌幅：
        正数：up_color
        负数：down_color
        0 或失败：neutral_color
    """
    setup_chinese_font()

    if title is None:
        title = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    output_path = Path(output_file)
    if output_path.parent and str(output_path.parent) != ".":
        output_path.parent.mkdir(parents=True, exist_ok=True)

    table_df = result_df.copy()
    table_df["今日预估涨跌幅"] = table_df["今日预估涨跌幅"].map(
        lambda x: format_pct(x, digits=pct_digits)
    )

    nrows = len(table_df)
    ncols = len(table_df.columns)

    # 画布高度随行数增长。比旧版更紧凑，避免标题/备注离表格过远。
    fig_h = max(1.8, row_height * (nrows + 1) + 0.45)

    if figure_width is None:
        fig_w = 14.0 if ncols >= 5 else 12.5
    else:
        fig_w = figure_width

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.axis("off")

    # 让轴区域占据大部分画布；真正的标题和备注位置后续根据表格边界计算。
    # fig.subplots_adjust(left=0.035, right=0.965, top=0.94, bottom=0.12)
    fig.subplots_adjust(left=0.015, right=0.985, top=0.985, bottom=0.015)

    # 先不直接画水印；等表格创建完成后，在表格区域内部平铺多个浅色水印。
    watermark_artists = []

    # 为标题和备注预留很小的区域，主体交给表格
    top_reserved = 0.08 if title else 0.03
    has_benchmark_footer = bool(benchmark_footer_items)
    if footnote_text and has_benchmark_footer:
        bottom_reserved = 0.13
    elif footnote_text or has_benchmark_footer:
        bottom_reserved = 0.09
    else:
        bottom_reserved = 0.03

    table_bbox = [0.02, bottom_reserved, 0.96, 1 - top_reserved - bottom_reserved]

    table = ax.table(
        cellText=table_df.values,
        colLabels=table_df.columns,
        cellLoc="center",
        colLoc="center",
        bbox=table_bbox,   # 用 bbox，不再用 loc="center"
        zorder=2,
    )

    table.auto_set_font_size(False)
    table.set_fontsize(17)
    table.scale(1.0, 1.22)

    est_col_idx = list(table_df.columns).index("今日预估涨跌幅")

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor(grid_color)
        cell.set_linewidth(0.8)

        if row == 0:
            cell.set_facecolor(header_bg)
            cell.set_text_props(color=header_text_color, weight="bold")
        else:
            cell.set_facecolor("white")

            if col == est_col_idx:
                raw_val = result_df.iloc[row - 1]["今日预估涨跌幅"]

                if raw_val is not None and not pd.isna(raw_val):
                    if raw_val > 0:
                        cell.get_text().set_color(up_color)
                        cell.get_text().set_weight("bold")
                    elif raw_val < 0:
                        cell.get_text().set_color(down_color)
                        cell.get_text().set_weight("bold")
                    else:
                        cell.get_text().set_color(neutral_color)
                else:
                    cell.get_text().set_color(neutral_color)

    # 按列名动态设置列宽
    col_width_by_name = {
        "序号": 0.06,
        "基金代码": 0.10,
        "基金名称": 0.37,
        "今日预估涨跌幅": 0.15,
        "限购金额": 0.15,
        "估算方式": 0.16,
    }

    for (row, col), cell in table.get_celld().items():
        if col < len(table_df.columns):
            col_name = table_df.columns[col]
            if col_name in col_width_by_name:
                cell.set_width(col_width_by_name[col_name])

    # ------------------------------------------------------------
    # 多个平铺水印：画在表格区域内部，透明度较低，但位于表格之上，保证可见
    # ------------------------------------------------------------
    if watermark_text and watermark_rows > 0 and watermark_cols > 0:
        table_left, table_bottom, table_width, table_height = table_bbox

        for r in range(int(watermark_rows)):
            for c in range(int(watermark_cols)):
                x = table_left + table_width * (c + 0.5) / float(watermark_cols)
                y = table_bottom + table_height * (r + 0.5) / float(watermark_rows)

                wm = ax.text(
                    x,
                    y,
                    watermark_text,
                    transform=ax.transAxes,
                    fontsize=watermark_fontsize,
                    color=watermark_color,
                    alpha=watermark_alpha,
                    ha="center",
                    va="center",
                    rotation=watermark_rotation,
                    zorder=watermark_zorder,
                )
                wm.set_in_layout(False)
                watermark_artists.append(wm)

    # ------------------------------------------------------------
    # 自适应标题与备注位置
    # ------------------------------------------------------------
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()

    bbox_disp = table.get_window_extent(renderer=renderer)
    bbox_fig = bbox_disp.transformed(fig.transFigure.inverted())

    table_top = bbox_fig.y1
    table_bottom = bbox_fig.y0
    table_height = max(table_top - table_bottom, 0.01)

    title_gap = max(
        min(table_height * title_gap_ratio, title_gap_max),
        title_gap_min,
    )
    footnote_gap = max(
        min(table_height * footnote_gap_ratio, footnote_gap_max),
        footnote_gap_min,
    )

    title_artist = None
    footnote_artist = None
    benchmark_footer_artist = None

    if title:
        title_y = min(table_top + title_gap, 0.985)
        title_artist = fig.text(
            0.5,
            title_y,
            title,
            ha="center",
            va="bottom",
            fontsize=title_fontsize,
            color=title_color,
            fontweight=title_fontweight,
        )

    footnote_y = None
    # ------------------------------------------------------------
    # 底部区域顺序：
    #   第一行：最新交易日指数基准；
    #   第二行：备注内容。
    # ------------------------------------------------------------
    benchmark_line_y = None
    footnote_line_y = None

    if benchmark_footer_items:
        benchmark_line_y = max(table_bottom - footnote_gap, 0.040 if footnote_text else 0.020)

        children = [
            TextArea(
                "基准：",
                textprops={"fontsize": benchmark_footer_fontsize, "color": footnote_color},
            )
        ]

        for idx, item in enumerate(benchmark_footer_items):
            if idx > 0:
                children.append(
                    TextArea("；", textprops={"fontsize": benchmark_footer_fontsize, "color": footnote_color})
                )

            label = str(item.get("label", "基准"))
            trade_date = str(item.get("trade_date", "")).strip()
            r_pct = item.get("return_pct")

            if r_pct is None or pd.isna(r_pct):
                seg_text = f"{label} 获取失败"
                seg_color = neutral_color
            else:
                # 日期必须显式显示在每个指数后，避免两个指数交易日不一致时产生歧义。
                seg_text = f"{label}（{trade_date or '日期未知'}）{format_pct(r_pct, digits=2)}"
                seg_color = up_color if float(r_pct) > 0 else (down_color if float(r_pct) < 0 else neutral_color)

            children.append(
                TextArea(
                    seg_text,
                    textprops={
                        "fontsize": benchmark_footer_fontsize,
                        "color": seg_color,
                        "fontweight": "bold",
                    },
                )
            )

        footer_pack = HPacker(children=children, align="center", pad=0, sep=2)
        benchmark_footer_artist = AnchoredOffsetbox(
            loc="center",
            child=footer_pack,
            pad=0.0,
            frameon=False,
            bbox_to_anchor=(0.5, benchmark_line_y),
            bbox_transform=fig.transFigure,
            borderpad=0.0,
        )
        fig.add_artist(benchmark_footer_artist)

    if footnote_text:
        if benchmark_line_y is not None:
            footnote_line_y = max(benchmark_line_y - 0.030, 0.010)
        else:
            footnote_line_y = max(table_bottom - footnote_gap, 0.015)

        footnote_artist = fig.text(
            0.5,
            footnote_line_y,
            f"备注：{footnote_text}",
            ha="center",
            va="top",
            fontsize=footnote_fontsize,
            color=footnote_color,
        )

    # 使用 bbox_extra_artists 确保标题和备注被纳入裁剪范围；
    # 不使用 tight_layout，避免再次把标题和备注推远。
    extra_artists = []
    if title_artist is not None:
        extra_artists.append(title_artist)
    if footnote_artist is not None:
        extra_artists.append(footnote_artist)
    if benchmark_footer_artist is not None:
        extra_artists.append(benchmark_footer_artist)
    extra_artists.extend(watermark_artists)

    fig.savefig(
        output_file,
        dpi=dpi,
        bbox_inches="tight",
        bbox_extra_artists=extra_artists,
        pad_inches=pad_inches,
    )
    plt.close(fig)

    print(f"基金预估收益表格图片已保存: {output_file}")



def build_benchmark_rows(
    benchmark_components,
    include_purchase_limit=True,
    manual_returns_pct=None,
    prefer_intraday=True,
    us_realtime=False,
    hk_realtime=False,
    security_return_cache_enabled=True,
    cn_hk_hourly_cache=True,
    valuation_mode="last_close",
):
    """
    构造市场基准行，用于追加到基金收益表。

    为了稳定可获取，纳斯达克100建议用 QQQ 代理，标普500用 SPY 代理。
    """
    if not benchmark_components:
        return []

    rows = []
    for i, comp in enumerate(benchmark_components, start=1):
        name = str(comp.get("name", comp.get("code", f"基准{i}"))).strip()
        code = str(comp.get("code", "")).strip()
        market = str(comp.get("market", "US")).strip().upper()
        display_code = str(comp.get("display_code", code)).strip()

        try:
            r_pct, source = get_stock_return_pct(
                market=market,
                ticker=code,
                manual_returns_pct=manual_returns_pct,
                prefer_intraday=prefer_intraday,
                us_realtime=us_realtime,
                hk_realtime=hk_realtime,
                security_return_cache_enabled=security_return_cache_enabled,
                cn_hk_hourly_cache=cn_hk_hourly_cache,
                valuation_mode=valuation_mode,
            )
            method = f"benchmark_{source}"
        except Exception as e:
            print(f"[WARN] 市场基准 {name}({code}) 获取失败: {e}", flush=True)
            r_pct = None
            method = "benchmark_failed"

        row = {
            "基金代码": display_code,
            "基金名称": name,
            "今日预估涨跌幅": r_pct,
            "_估算方式": method,
        }

        if include_purchase_limit:
            row["限购金额"] = "—"

        rows.append(row)

    return rows


def _attach_benchmark_rows_to_result_df(
    result_df,
    benchmark_rows,
    include_purchase_limit=True,
    include_method_col=False,
    benchmark_position="top",
):
    """
    将市场基准行追加到收益表，并重新编号。
    """
    if not benchmark_rows:
        return result_df

    bench_df = pd.DataFrame(benchmark_rows)

    cols = ["序号", "基金代码", "基金名称", "今日预估涨跌幅"]
    if include_purchase_limit:
        cols.append("限购金额")
    if include_method_col:
        bench_df["估算方式"] = bench_df.get("_估算方式", "benchmark")
        cols.append("估算方式")

    for col in cols:
        if col not in bench_df.columns:
            bench_df[col] = "" if col != "今日预估涨跌幅" else None
        if col not in result_df.columns:
            result_df[col] = "" if col != "今日预估涨跌幅" else None

    bench_df = bench_df[cols]
    result_df = result_df[cols]

    if str(benchmark_position).strip().lower() == "bottom":
        out = pd.concat([result_df, bench_df], ignore_index=True)
    else:
        out = pd.concat([bench_df, result_df], ignore_index=True)

    out["序号"] = range(1, len(out) + 1)
    return out




def _is_overseas_fund_table_context(title=None, output_file=None) -> bool:
    """
    判断当前表格是否为“海外市场基金收益表”。

    只用于自动启用海外股票持仓型基金的剩余仓位纳斯达克100补偿口径。
    不改变国内基金表，不改变 DEFAULT_FUND_PROXY_MAP 中 ETF/FOF/指数代理基金的计算逻辑。
    """
    texts = []
    if title is not None:
        texts.append(str(title))
    if output_file is not None:
        texts.append(str(output_file))

    joined = " ".join(texts).lower()
    return any(key in joined for key in ["海外", "haiwai", "global", "qdii", "oversea", "overseas"])

def estimate_funds_and_save_table(
    fund_codes,
    top_n=10,
    output_file="output/fund_estimate_table.png",
    title=None,
    manual_returns_pct=None,
    prefer_intraday=True,
    us_realtime=False,
    hk_realtime=False,
    renormalize_available_holdings=True,
    include_purchase_limit=True,
    purchase_limit_timeout=8,
    purchase_limit_cache_days=7,
    holding_cache_days=75,
    cache_enabled=True,
    security_return_cache_enabled=True,
    cn_hk_hourly_cache=True,
    print_table=True,
    save_table=True,
    watermark_text="鱼师",
    watermark_alpha=0.06,
    watermark_fontsize=32, # 控制水印字体大小
    watermark_rotation=28, # 控制水印旋转角度
    watermark_rows=4,      # 水印行数
    watermark_cols=3,      # 水印列数
    watermark_color="#000000FC", # 水印颜色
    watermark_zorder=3,    # 水印层级（高于表格，保证可见）
    up_color="red",
    down_color="green",
    neutral_color="black",
    pct_digits=2,
    dpi=220,
    header_bg="#2f3b52",
    header_text_color="white",
    grid_color="#d9d9d9",
    figure_width=None,
    row_height=0.55,
    footnote_text="按照基金公告披露的持仓股涨跌幅及仓位预估基金收益率",
    footnote_color="#666666",
    footnote_fontsize=15, # 控制备注字体大小
    benchmark_footer_items=None,
    benchmark_footer_fontsize=15,
    title_fontsize=20, # 控制标题字体大小
    title_color="black",
    title_fontweight="bold",
    title_gap_ratio=0.02,
    title_gap_min=0.008,
    title_gap_max=0.026,
    footnote_gap_ratio=0.02,
    footnote_gap_min=0.008,
    footnote_gap_max=0.026,
    pad_inches=0.14,
    sort_by_return=True,
    holding_mode="auto",
    proxy_map=None,
    proxy_normalize_weights=False,
    include_method_col=False,
    valuation_mode="intraday",
    stock_residual_benchmark=None,
    stock_residual_benchmark_return_pct=None,
    stock_residual_benchmark_label=None,
    stock_residual_benchmark_source=None,
    benchmark_components=None,
    benchmark_position="top",
):
    """
    一站式入口：估算多个基金、打印表格、保存图片。

    主要参数
    --------
    fund_codes:
        基金代码。可传字符串或列表。
        示例："017437" 或 ["017437", "007467", "015016"]

    top_n:
        股票型基金取前 N 大股票持仓，并归一化到 100%。

    holding_mode:
        "auto"：
            fund_code 在 proxy_map 中时，走 ETF/FOF 代理估算；
            否则走股票持仓估算。
        "stock"：
            强制股票持仓估算。
        "proxy"：
            强制 ETF/FOF 代理估算。

    proxy_map:
        代理映射表。None 时使用 DEFAULT_FUND_PROXY_MAP。
        新增 ETF 联接基金时，优先改 DEFAULT_FUND_PROXY_MAP。

    proxy_normalize_weights:
        False：
            默认。ETF/FOF 代理按原始仓位计算，现金仓位按 0。
        True：
            将代理组件归一化到 100%。

    sort_by_return:
        True：
            按今日预估涨跌幅从高到低排序，并重新编号。
        False：
            保留输入顺序。

    include_method_col:
        True：显示估算方式列。
        False：不显示，表格更简洁。

    us_realtime:
        False：默认快速模式，美股用单只股票日线。
        True ：盘中实时模式，美股优先新浪/东方财富实时，可能较慢。

    hk_realtime:
        False：默认使用港股历史日线，避免全市场行情。
        True ：先尝试新浪单只港股实时行情，再回落到港股日线。

    renormalize_available_holdings:
        True：
            如果某些持仓股无法获取行情，则剔除这些持仓，
            并把剩余可获取行情的持仓重新归一化到 100% 后估算收益。
            适合日东纺这类行情缺失的情况。
        False：
            保留原前 N 大归一化权重，不把缺失仓位重新分配给可查持仓。

    title_gap_ratio / title_gap_min / title_gap_max:
        标题与表格上边界之间的自适应距离。数值越小，标题越贴近表格。

    footnote_gap_ratio / footnote_gap_min / footnote_gap_max:
        备注与表格下边界之间的自适应距离。数值越小，备注越贴近表格。

    pad_inches:
        保存图片时的外边距。数值越小，整张图片留白越少。
    """
    auto_overseas_residual_enabled = _is_overseas_fund_table_context(
        title=title,
        output_file=output_file,
    )

    if title is None:
        title = "海外市场收益预估"+ datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if proxy_map is None:
        proxy_map = DEFAULT_FUND_PROXY_MAP

    # 股票持仓型基金的“剩余仓位基准补偿”。
    # 自动规则：仅当当前表格被识别为“海外市场基金收益表”时启用纳斯达克100补偿；
    # 国内表不启用；DEFAULT_FUND_PROXY_MAP 中 ETF/FOF/指数代理基金仍保持原计算逻辑。
    if (
        stock_residual_benchmark is None
        and stock_residual_benchmark_return_pct is None
        and auto_overseas_residual_enabled
    ):
        stock_residual_benchmark = "nasdaq100"

    if stock_residual_benchmark and stock_residual_benchmark_return_pct is None:
        bench_key = str(stock_residual_benchmark).strip().lower()
        if bench_key in {"nasdaq100", "nasdaq_100", "ndx", ".ndx"}:
            try:
                r_pct, trade_date, source = get_us_index_return_pct_cached(
                    symbol=".NDX",
                    display_name="纳斯达克100",
                    cache_enabled=cache_enabled,
                )
                stock_residual_benchmark_return_pct = r_pct
                stock_residual_benchmark_label = stock_residual_benchmark_label or f"纳斯达克100({trade_date})"
                stock_residual_benchmark_source = stock_residual_benchmark_source or source
            except Exception as e:
                print(f"[WARN] 剩余仓位基准 {stock_residual_benchmark} 获取失败，将沿用原股票持仓估算口径: {e}", flush=True)
        else:
            print(f"[WARN] 未识别的 stock_residual_benchmark={stock_residual_benchmark!r}，将沿用原股票持仓估算口径。", flush=True)

    result_df, detail_map = estimate_funds(
        fund_codes=fund_codes,
        top_n=top_n,
        manual_returns_pct=manual_returns_pct,
        prefer_intraday=prefer_intraday,
        us_realtime=us_realtime,
        hk_realtime=hk_realtime,
        renormalize_available_holdings=renormalize_available_holdings,
        include_purchase_limit=include_purchase_limit,
        purchase_limit_timeout=purchase_limit_timeout,
        purchase_limit_cache_days=purchase_limit_cache_days,
        holding_cache_days=holding_cache_days,
        cache_enabled=cache_enabled,
        security_return_cache_enabled=security_return_cache_enabled,
        cn_hk_hourly_cache=cn_hk_hourly_cache,
        sort_by_return=sort_by_return,
        holding_mode=holding_mode,
        proxy_map=proxy_map,
        proxy_normalize_weights=proxy_normalize_weights,
        include_method_col=include_method_col,
        valuation_mode=valuation_mode,
        stock_residual_benchmark_return_pct=stock_residual_benchmark_return_pct,
        stock_residual_benchmark_label=stock_residual_benchmark_label,
        stock_residual_benchmark_source=stock_residual_benchmark_source,
    )

    benchmark_rows = build_benchmark_rows(
        benchmark_components=benchmark_components,
        include_purchase_limit=include_purchase_limit,
        manual_returns_pct=manual_returns_pct,
        prefer_intraday=prefer_intraday,
        us_realtime=us_realtime,
        hk_realtime=hk_realtime,
        security_return_cache_enabled=security_return_cache_enabled,
        cn_hk_hourly_cache=cn_hk_hourly_cache,
        valuation_mode=valuation_mode,
    )

    result_df = _attach_benchmark_rows_to_result_df(
        result_df=result_df,
        benchmark_rows=benchmark_rows,
        include_purchase_limit=include_purchase_limit,
        include_method_col=include_method_col,
        benchmark_position=benchmark_position,
    )

    if print_table:
        print_fund_estimate_table(
            result_df,
            title=title,
            pct_digits=pct_digits,
        )

    if save_table:
        save_fund_estimate_table_image(
            result_df=result_df,
            output_file=output_file,
            title=title,
            dpi=dpi,
            watermark_text=watermark_text,
            watermark_alpha=watermark_alpha,
            watermark_fontsize=watermark_fontsize,
            watermark_rotation=watermark_rotation,
            watermark_rows=watermark_rows,
            watermark_cols=watermark_cols,
            watermark_color=watermark_color,
            watermark_zorder=watermark_zorder,
            up_color=up_color,
            down_color=down_color,
            neutral_color=neutral_color,
            pct_digits=pct_digits,
            header_bg=header_bg,
            header_text_color=header_text_color,
            grid_color=grid_color,
            figure_width=figure_width,
            row_height=row_height,
            footnote_text=footnote_text,
            footnote_color=footnote_color,
            footnote_fontsize=footnote_fontsize,
            benchmark_footer_items=benchmark_footer_items,
            benchmark_footer_fontsize=benchmark_footer_fontsize,
            title_fontsize=title_fontsize,
            title_color=title_color,
            title_fontweight=title_fontweight,
            title_gap_ratio=title_gap_ratio,
            title_gap_min=title_gap_min,
            title_gap_max=title_gap_max,
            footnote_gap_ratio=footnote_gap_ratio,
            footnote_gap_min=footnote_gap_min,
            footnote_gap_max=footnote_gap_max,
            pad_inches=pad_inches,
        )

    return result_df, detail_map


# ============================================================
# 9. 兼容旧函数名
# ============================================================

def get_jijin_holdings(
    fund_code="017437",
    top_n=10,
    estimate_return=True,
    manual_returns_pct=None,
    prefer_intraday=True,
    us_realtime=False,
    hk_realtime=False,
    renormalize_available_holdings=True,
    include_purchase_limit=True,
    purchase_limit_timeout=8,
    purchase_limit_cache_days=7,
    holding_cache_days=75,
    cache_enabled=True,
    security_return_cache_enabled=True,
    cn_hk_hourly_cache=True,
    return_summary=False,
    return_text=False,
    holding_mode="auto",
    proxy_map=None,
    proxy_normalize_weights=False,
    valuation_mode="intraday",
):
    """
    兼容旧调用方式。

    新项目建议直接使用：
        estimate_funds_and_save_table(...)
    """
    fund_code = str(fund_code).zfill(6)
    fund_name = get_fund_name(fund_code)

    if not estimate_return:
        latest_df = get_latest_stock_holdings_df(
            fund_code=fund_code,
            top_n=top_n,
            holding_cache_days=holding_cache_days,
            cache_enabled=cache_enabled,
        )
        print(latest_df)
        return latest_df

    result_row, estimate_df, summary = estimate_one_fund(
        fund_code=fund_code,
        top_n=top_n,
        manual_returns_pct=manual_returns_pct,
        prefer_intraday=prefer_intraday,
        us_realtime=us_realtime,
        hk_realtime=hk_realtime,
        renormalize_available_holdings=renormalize_available_holdings,
        include_purchase_limit=include_purchase_limit,
        purchase_limit_timeout=purchase_limit_timeout,
        purchase_limit_cache_days=purchase_limit_cache_days,
        holding_cache_days=holding_cache_days,
        cache_enabled=cache_enabled,
        security_return_cache_enabled=security_return_cache_enabled,
        cn_hk_hourly_cache=cn_hk_hourly_cache,
        holding_mode=holding_mode,
        proxy_map=proxy_map,
        proxy_normalize_weights=proxy_normalize_weights,
        valuation_mode=valuation_mode,
    )

    purchase_limit_text = ""
    if include_purchase_limit:
        purchase_limit_text = f"\n限购金额：{result_row.get('限购金额', '未知')}"

    estimate_text = (
        f"【基金收益预估】\n"
        f"基金代码：{fund_code}\n"
        f"基金名称：{fund_name}\n"
        f"估算方式：{summary.get('method', '')}\n"
        f"最终预估收益：{format_pct(summary['estimated_return_pct'], digits=4)}"
        f"{purchase_limit_text}"
    )

    print("=" * 100)
    print(estimate_text)
    print("=" * 100)

    if return_text:
        return estimate_df, summary, estimate_text

    if return_summary:
        return estimate_df, summary

    return estimate_df


if __name__ == "__main__":
    # ========================================================
    # 示例 1：混合估算
    # 017437：股票持仓估算
    # 007467：ETF 联接代理估算
    # 015016：DAX ETF 联接代理估算
    # 007722：标普500 FOF 指数代理估算
    # ========================================================
    estimate_funds_and_save_table(
        fund_codes=[
        "007467", # 华泰柏瑞中证红利低波
        "015311", # 华泰柏瑞南方东英恒生科技指数
        "005125", # 华宝标普中国A股红利指数
        "019127", # 华泰柏瑞港股通医疗精选混合
        "023918", # 华夏国证自由现金流
        "008987", # 广发上海金ETF
        "014143", # 银河创新成长混合
        "025196", # 广发创业板指数增强
        "010238", # 安信创新先锋混合
        "013881", # 长信中证500指数增强
        "016020", # 招商中证电池主题ETF
        "025924", # 平安新能源精选混合
        "012414", # 招商中证白酒
        "110022", # 易方达消费行业股票
        "012725", # 国泰中证畜牧养殖
        "015850", # 宝盈国证证券龙头指数
        "023145", # 汇添富中证油气资源
        "011840", # 天弘中证人工智能主题
        "011103", # 天弘中证光伏产业
        "020691", # 博时中证全指通信设备指数
        ],
        top_n=10, # 股票持仓估算取前 10 大股票
        output_file="output/guonei_fund_estimate_table.png",
        title=None,
        holding_mode="auto", # 自动选择股票持仓或代理估算
        proxy_normalize_weights=False, # 代理按原始权重计算，现金按 0
        us_realtime=False,  # 如果开启实时数据，则会拉取所有的美股数据，耗时较长
        hk_realtime=True,   # 港股优先使用东方财富实时行情；失败后回落到日线
        renormalize_available_holdings=True,  # 某些持仓行情缺失时，用可查持仓重新归一化估算
        include_purchase_limit=True,
        include_method_col=False,
        sort_by_return=True,
        watermark_text="鱼师-发光发热",
        up_color="red",
        down_color="green",
        print_table=True,
        save_table=True,
    )

    # ========================================================

