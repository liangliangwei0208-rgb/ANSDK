"""
生成基金费用科普图：
申购费、赎回费、管理费、托管费，一张图看懂

"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw

KEPU_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = KEPU_DIR.parent

for import_path in (PROJECT_ROOT, KEPU_DIR):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

import first_pic as art


# 输出到 /kepu/series/kepu_fee.png
# 这里的 /kepu 指项目中的 kepu 文件夹，而不是 Linux 根目录 /kepu。
OUTPUT_FILE = KEPU_DIR / "series" / "kepu_fee.png"


def _normalize_today(value=None) -> date:
    if value is not None:
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except Exception as exc:
            raise ValueError("today 必须是可解析日期，例如 2026-05-02。") from exc

    try:
        return datetime.now(ZoneInfo("Asia/Shanghai")).date()
    except Exception:
        return datetime.now().date()


def _draw_strong_brand_watermarks(image: Image.Image) -> None:
    """
    更明显的全页品牌水印：
    - 只显示“鱼师AHNS”
    - 字号较大、透明度较高、斜向平铺
    - 适合抖音图文传播时保留品牌识别
    """
    overlay = Image.new("RGBA", image.size, (255, 255, 255, 0))

    text = "鱼师AHNS"
    font = art.load_font(150, bold=True)
    fill = (18, 24, 38, 42)

    patch_w, patch_h = 980, 280
    patch = Image.new("RGBA", (patch_w, patch_h), (255, 255, 255, 0))
    patch_draw = ImageDraw.Draw(patch)

    bbox = patch_draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]

    patch_draw.text(
        ((patch_w - tw) / 2 - bbox[0], (patch_h - th) / 2 - bbox[1]),
        text,
        font=font,
        fill=fill,
    )

    rotated = patch.rotate(28, expand=True, resample=Image.Resampling.BICUBIC)

    step_x = 700
    step_y = 430

    for row, y in enumerate(range(280, art.HEIGHT - 80, step_y)):
        offset_x = 0 if row % 2 == 0 else 320
        for x in range(-260 + offset_x, art.WIDTH + 260, step_x):
            overlay.alpha_composite(rotated, (x, y))

    image.alpha_composite(overlay)


def _draw_title(draw: ImageDraw.ImageDraw, today: date) -> None:
    art.draw_center_text(
        draw,
        art.WIDTH // 2,
        48,
        f"北京时间：{today.strftime('%Y-%m-%d')}",
        art.FONT_TITLE,
        art.INK,
    )
    art.draw_center_text(
        draw,
        art.WIDTH // 2,
        188,
        "申购费、赎回费、管理费、托管费，一张图看懂",
        art.FONT_TITLE,
        art.INK,
    )
    art.draw_center_text(
        draw,
        art.WIDTH // 2,
        328,
        "先看买入、持有、卖出时分别可能遇到什么费用",
        art.FONT_SUBTITLE,
        art.MUTED,
    )


def _draw_fee_intro_section(draw: ImageDraw.ImageDraw) -> None:
    """
    第一部分：四种费用分别是什么
    """
    left, content_top, right, section_bottom = art.draw_section_shell(
        draw,
        440,
        760,
        "1｜四种常见费用分别是什么？",
    )

    start_x = left + 110
    card_gap = 48
    card_w = int((right - left - 220 - card_gap * 3) / 4)
    card_y = content_top + 55
    card_h = section_bottom - card_y - 60

    cards = [
        (
            "申购费",
            art.BLUE,
            "买入基金时\n可能发生的费用\n很多 A 类基金更常见",
            "买进去时看",
        ),
        (
            "赎回费",
            art.GOLD,
            "卖出基金时\n可能发生的费用\n短期持有通常更要注意",
            "卖出来时看",
        ),
        (
            "管理费",
            art.GREEN,
            "基金公司管理产品\n收取的费用\n通常已体现在净值里",
            "持有中会有",
        ),
        (
            "托管费",
            art.RED,
            "托管银行保管和监督\n基金资产运作的费用\n通常也体现在净值里",
            "持有中会有",
        ),
    ]

    for idx, (title, color, body, tag) in enumerate(cards):
        x = start_x + idx * (card_w + card_gap)

        art.rounded(
            draw,
            (x, card_y, x + card_w, card_y + card_h),
            28,
            art.SOFT_CARD,
            art.LINE,
            2,
        )

        art.rounded(
            draw,
            (x + 26, card_y + 30, x + card_w - 26, card_y + 110),
            24,
            color,
            None,
        )

        art.draw_text_in_box(
            draw,
            (x + 40, card_y + 34, x + card_w - 40, card_y + 106),
            title,
            font_size=56,
            min_font_size=42,
            bold=True,
            fill="white",
            align="center",
            valign="center",
        )

        art.rounded(
            draw,
            (x + 36, card_y + 135, x + card_w - 36, card_y + 198),
            22,
            "#ffffff",
            art.LINE,
            2,
        )

        art.draw_text_in_box(
            draw,
            (x + 48, card_y + 143, x + card_w - 48, card_y + 190),
            tag,
            font_size=46,
            min_font_size=34,
            bold=True,
            fill=color,
            align="center",
            valign="center",
        )

        art.draw_text_in_box(
            draw,
            (x + 34, card_y + 220, x + card_w - 34, card_y + card_h - 32),
            body,
            font_size=48,
            min_font_size=36,
            bold=True,
            fill=art.INK,
            align="center",
            valign="center",
            line_gap=12,
        )


def _draw_timing_section(draw: ImageDraw.ImageDraw) -> None:
    """
    第二部分：这些费用在什么时候出现
    """
    left, content_top, right, section_bottom = art.draw_section_shell(
        draw,
        1240,
        800,
        "2｜这些费用一般在什么时候出现？",
    )

    start_x = left + 120
    card_gap = 70
    card_w = int((right - left - 240 - card_gap * 2) / 3)
    card_y = content_top + 60
    card_h = section_bottom - card_y - 160

    cards = [
        (
            "买入时",
            art.BLUE,
            [
                "可能看到申购费",
                "并不是每只基金都会收",
                "很多 A 类基金更常见",
            ],
        ),
        (
            "持有中",
            art.GREEN,
            [
                "会有管理费、托管费",
                "通常按日计提",
                "一般已体现在净值里",
            ],
        ),
        (
            "卖出时",
            art.GOLD,
            [
                "可能看到赎回费",
                "持有越短越要注意",
                "先看持有天数要求",
            ],
        ),
    ]

    for idx, (title, color, bullets) in enumerate(cards):
        x = start_x + idx * (card_w + card_gap)

        art.rounded(
            draw,
            (x, card_y, x + card_w, card_y + card_h),
            30,
            art.SOFT_CARD,
            art.LINE,
            2,
        )

        art.rounded(
            draw,
            (x + 34, card_y + 36, x + card_w - 34, card_y + 118),
            28,
            color,
            None,
        )

        art.draw_text_in_box(
            draw,
            (x + 52, card_y + 40, x + card_w - 52, card_y + 114),
            title,
            font_size=62,
            min_font_size=46,
            bold=True,
            fill="white",
            align="center",
            valign="center",
        )

        art.draw_bullets_in_box(
            draw,
            (x + 60, card_y + 160, x + card_w - 42, card_y + card_h - 42),
            bullets,
            color,
            font_size=53,
            min_font_size=42,
            gap=18,
            valign="center",
        )

    note_box = (left + 140, section_bottom - 112, right - 140, section_bottom - 34)
    art.rounded(draw, note_box, 24, "#fff8eb", "#e8cf9e", 2)

    art.draw_text_in_box(
        draw,
        (note_box[0] + 35, note_box[1] + 8, note_box[2] - 35, note_box[3] - 8),
        "管理费、托管费通常不是额外再从银行卡单独扣一笔，而是一般已经体现在基金净值里。",
        font_size=52,
        min_font_size=40,
        bold=True,
        fill=art.INK,
        align="center",
        valign="center",
    )


def _draw_reminder_section(draw: ImageDraw.ImageDraw) -> None:
    """
    第三部分：小白最该记住什么
    """
    left, content_top, right, section_bottom = art.draw_section_shell(
        draw,
        2070,
        830,
        "3｜小白看基金费用，重点记住什么？",
    )

    inner = (left + 120, content_top + 58, right - 120, section_bottom - 170)
    art.rounded(draw, inner, 30, "#ffffff", art.LINE, 2)

    bullets = [
        "先看基金份额：A 类和 C 类的收费结构常常不同，不要混着看。",
        "不要只盯着申购费；如果持有时间很短，赎回费有时更值得注意。",
        "管理费、托管费通常已经反映在净值里，不是另外再弹出一笔扣款。",
        "部分基金还可能有销售服务费等，本图只讲最常见的四类费用。",
    ]

    art.draw_bullets_in_box(
        draw,
        (inner[0] + 80, inner[1] + 42, inner[2] - 70, inner[3] - 40),
        bullets,
        art.RED,
        font_size=56,
        min_font_size=44,
        gap=20,
        valign="center",
    )

    footer_tip = (left + 120, section_bottom - 132, right - 120, section_bottom - 42)
    art.rounded(draw, footer_tip, 26, "#fffdf8", "#e7d0a3", 2)

    art.draw_text_in_box(
        draw,
        (footer_tip[0] + 40, footer_tip[1] + 10, footer_tip[2] - 40, footer_tip[3] - 10),
        "最终收费规则请以基金详情页、招募说明书、基金合同和销售平台展示为准。",
        font_size=58,
        min_font_size=44,
        bold=True,
        fill=art.RED,
        align="center",
        valign="center",
    )


def _draw_footer(draw: ImageDraw.ImageDraw, top: int = 3000) -> None:
    footer_box = (150, top, art.WIDTH - 150, top + 130)
    art.rounded(draw, footer_box, 28, "#fffdf8", "#e7d0a3", 2)

    art.draw_text_in_box(
        draw,
        (footer_box[0] + 45, footer_box[1] + 20, footer_box[2] - 45, footer_box[3] - 20),
        "仅供个人学习记录，不构成任何投资建议。",
        font_size=78,
        min_font_size=56,
        bold=True,
        fill=art.RED,
        align="center",
        valign="center",
    )

    signature = "鱼师AHNS · 个人公开数据建模复盘"
    sw, _ = art.text_size(draw, signature, art.FONT_SIGNATURE)
    draw.text((art.WIDTH - 150 - sw, top + 145), signature, font=art.FONT_SIGNATURE, fill="#7b8796")


def build_image(today: date) -> Image.Image:
    image = Image.new("RGBA", (art.WIDTH, art.HEIGHT), art.BG)

    _draw_strong_brand_watermarks(image)

    draw = ImageDraw.Draw(image)
    _draw_title(draw, today)
    _draw_fee_intro_section(draw)
    _draw_timing_section(draw)
    _draw_reminder_section(draw)
    _draw_footer(draw)

    return image.convert("RGB")


def run(today=None) -> bool:
    today_date = _normalize_today(today)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    image = build_image(today_date)
    image.save(OUTPUT_FILE, optimize=True, compress_level=9)

    print(f"基金费用科普图已生成: {OUTPUT_FILE.resolve()}")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成基金费用科普图")
    parser.add_argument(
        "--today",
        default=None,
        help="用于测试的北京时间日期，例如 2026-05-02；默认使用今天。",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(today=args.today)


if __name__ == "__main__":
    main()