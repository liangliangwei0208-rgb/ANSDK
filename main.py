from datetime import datetime
import pandas as pd

from stock_analysis import build_stock_analysis
from tools.quote_manager import get_daily_quote_text
from tools.email_send import send_email


# def latest_trade_date(df):
#     if df is None or df.empty or "date" not in df.columns:
#         return None

#     dates = pd.to_datetime(df["date"], errors="coerce").dropna()

#     if dates.empty:
#         return None

#     return dates.max().date()


# def is_trade_day(results):
#     """
#     用 A 股 ETF 判断今天是否为交易日。
#     只要红利低波或上证 ETF 任意一个最新交易日为今天，就认为今天有效。
#     """
#     today = datetime.now().date()

#     check_keywords = [
#         "红利低波",
#         "上证指数ETF",
#         "510210",
#     ]

#     trade_dates = []

#     for item in results:
#         if any(k in item.name for k in check_keywords):
#             d = latest_trade_date(item.hist)
#             trade_dates.append((item.name, d))

#     print("交易日检查：", trade_dates)

#     return any(d == today for _, d in trade_dates)


# # 获得分析结果
# stock_text, image_paths, results = build_stock_analysis(
#     return_raw=True,
#     include_factors=True,
#     include_realtime=True,
# )

# # 如果不是交易日，不发送邮件
# if not is_trade_day(results):
#     print("今天不是有效交易日，或行情没有更新到今天，本次不发送邮件。")
#     raise SystemExit(0)

# # 获得每日语录
# quote_text = get_daily_quote_text(
#     quote_file="investment_quotes.txt",
#     history_file="investment_quote_history.json",
# )

# now = datetime.now()

# if now.hour < 14:
#     time_note = "注：本邮件包含午盘盘中临时行情，RSI 与量化因子并非收盘确认值。"
# else:
#     time_note = "注：本邮件为收盘后或接近收盘后的行情摘要。"

# email_text = quote_text + "\n\n" + time_note + "\n\n" + stock_text

# print(email_text)

# send_email(
#     subject=f"发光发热—每日提醒——分析结果—{now.strftime('%Y-%m-%d %H:%M')}",
#     text=email_text,
#     image_paths=image_paths,
#     to_email="2569236501@qq.com",
# )

send_email(
     subject="123",
     text="123",
     to_email="2569236501@qq.com",
 )
