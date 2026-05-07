"""
生成基金小白科普图：
什么是基金？基金有哪些分类？

"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont


KEPU_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = KEPU_DIR.parent

for import_path in (PROJECT_ROOT, KEPU_DIR):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

import first_pic as art


OUTPUT_FILE = KEPU_DIR / "series" / "kepu_fund_intro.png"


# ============================================================
# 日期
# ============================================================

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


# ============================================================
# 精准文字布局工具
# ============================================================

def _text_bbox(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
) -> tuple[int, int, int, int]:
    return draw.textbbox((0, 0), text, font=font)


def _text_size(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
) -> tuple[int, int]:
    bbox = _text_bbox(draw, text, font)
    return int(bbox[2] - bbox[0]), int(bbox[3] - bbox[1])


def _draw_center_text(
    draw: ImageDraw.ImageDraw,
    center_x: int,
    y: int,
    text: str,
    font: ImageFont.ImageFont,
    fill: str,
) -> None:
    bbox = _text_bbox(draw, text, font)
    text_w = bbox[2] - bbox[0]
    draw_x = center_x - text_w / 2 - bbox[0]
    draw_y = y - bbox[1]
    draw.text((draw_x, draw_y), text, font=font, fill=fill)


def _wrap_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
) -> list[str]:
    lines: list[str] = []

    for para in text.split("\n"):
        if para == "":
            lines.append("")
            continue

        current = ""
        for char in para:
            candidate = current + char
            if _text_size(draw, candidate, font)[0] <= max_width:
                current = candidate
            else:
                if current:
                    lines.append(current)
                current = char

        if current:
            lines.append(current)

    return lines


def _measure_wrapped_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
    line_gap: int,
) -> tuple[int, list[str], list[tuple[int, int, int, int]]]:
    lines = _wrap_text(draw, text, font, max_width)

    if not lines:
        return 0, [], []

    boxes: list[tuple[int, int, int, int]] = []
    total_h = 0

    for idx, line in enumerate(lines):
        bbox = _text_bbox(draw, line, font)
        boxes.append(bbox)

        line_h = bbox[3] - bbox[1]
        total_h += line_h

        if idx < len(lines) - 1:
            total_h += line_gap

    return total_h, lines, boxes


def _draw_text_in_box(
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
    text: str,
    *,
    font_size: int,
    min_font_size: int,
    bold: bool,
    fill: str,
    line_gap: int = 10,
    align: str = "left",
    valign: str = "top",
) -> None:
    """
    在指定区域内绘制文本，并做视觉居中。
    """
    left, top, right, bottom = box
    max_width = max(10, right - left)
    max_height = max(10, bottom - top)

    chosen_font = art.load_font(font_size, bold=bold)
    chosen_lines: list[str] = []
    chosen_boxes: list[tuple[int, int, int, int]] = []
    chosen_height = 0

    for size in range(font_size, min_font_size - 1, -2):
        font = art.load_font(size, bold=bold)
        height, lines, boxes = _measure_wrapped_text(
            draw,
            text,
            font,
            max_width,
            line_gap,
        )

        chosen_font = font
        chosen_lines = lines
        chosen_boxes = boxes
        chosen_height = height

        if height <= max_height:
            break

    if not chosen_lines:
        return

    y = top
    if valign == "center":
        y = top + max(0, (max_height - chosen_height) // 2)

    for line, bbox in zip(chosen_lines, chosen_boxes):
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]

        if align == "center":
            visual_left = left + (max_width - text_w) / 2
        elif align == "right":
            visual_left = right - text_w
        else:
            visual_left = left

        draw_x = visual_left - bbox[0]
        draw_y = y - bbox[1]

        draw.text((draw_x, draw_y), line, font=chosen_font, fill=fill)

        y += text_h + line_gap
        if y > bottom:
            break


def _measure_bullets(
    draw: ImageDraw.ImageDraw,
    bullets: list[str],
    font: ImageFont.ImageFont,
    max_width: int,
    line_gap: int,
    gap: int,
) -> tuple[int, list[list[str]], list[list[tuple[int, int, int, int]]]]:
    wrapped: list[list[str]] = []
    wrapped_boxes: list[list[tuple[int, int, int, int]]] = []

    total_h = 0
    text_width = max(10, max_width - 54)

    for idx, text in enumerate(bullets):
        lines = _wrap_text(draw, text, font, text_width)
        boxes = [_text_bbox(draw, line, font) for line in lines]

        wrapped.append(lines)
        wrapped_boxes.append(boxes)

        bullet_h = 0
        for line_idx, bbox in enumerate(boxes):
            line_h = bbox[3] - bbox[1]
            bullet_h += line_h
            if line_idx < len(boxes) - 1:
                bullet_h += line_gap

        total_h += bullet_h
        if idx < len(bullets) - 1:
            total_h += gap

    return total_h, wrapped, wrapped_boxes


def _draw_bullets_in_box(
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
    bullets: list[str],
    dot_color: str,
    *,
    font_size: int,
    min_font_size: int = 30,
    bold: bool = True,
    fill: str = art.INK,
    line_gap: int = 6,
    gap: int = 16,
    valign: str = "top",
) -> None:
    left, top, right, bottom = box
    max_width = max(10, right - left)
    max_height = max(10, bottom - top)

    chosen_font = art.load_font(font_size, bold=bold)
    chosen_wrapped: list[list[str]] = []
    chosen_wrapped_boxes: list[list[tuple[int, int, int, int]]] = []
    chosen_height = 0

    for size in range(font_size, min_font_size - 1, -2):
        font = art.load_font(size, bold=bold)
        height, wrapped, wrapped_boxes = _measure_bullets(
            draw,
            bullets,
            font,
            max_width,
            line_gap,
            gap,
        )

        chosen_font = font
        chosen_wrapped = wrapped
        chosen_wrapped_boxes = wrapped_boxes
        chosen_height = height

        if height <= max_height:
            break

    y = top
    if valign == "center":
        y = top + max(0, (max_height - chosen_height) // 2)

    for bullet_lines, bullet_boxes in zip(chosen_wrapped, chosen_wrapped_boxes):
        if not bullet_lines:
            continue

        first_box = bullet_boxes[0]
        first_line_h = first_box[3] - first_box[1]
        dot_size = 20

        dot_y = y + max(0, (first_line_h - dot_size) // 2)
        draw.ellipse((left, dot_y, left + dot_size, dot_y + dot_size), fill="#dbeafe")
        draw.ellipse((left + 5, dot_y + 5, left + 15, dot_y + 15), fill=dot_color)

        line_y = y

        for line, bbox in zip(bullet_lines, bullet_boxes):
            line_h = bbox[3] - bbox[1]

            draw.text(
                (left + 54 - bbox[0], line_y - bbox[1]),
                line,
                font=chosen_font,
                fill=fill,
            )

            line_y += line_h + line_gap

            if line_y > bottom:
                return

        y = line_y - line_gap + gap

        if y > bottom:
            return


# ============================================================
# 水印与公共结构
# ============================================================

def _draw_strong_brand_watermarks(image: Image.Image) -> None:
    """
    全页品牌水印：
    - 只显示“鱼师AHNS”
    - 明显但不遮挡正文
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


def _draw_section_shell(
    draw: ImageDraw.ImageDraw,
    top: int,
    height: int,
    title: str,
) -> tuple[int, int, int, int]:
    left, right = 150, art.WIDTH - 150
    bottom = top + height

    art.rounded(draw, (left, top, right, bottom), 38, art.CARD_BG, art.LINE, 2)
    draw.rounded_rectangle((left, top, right, top + 130), radius=38, fill=art.NAVY)
    draw.rectangle((left, top + 72, right, top + 130), fill=art.NAVY)

    section_font = art.load_font(66, bold=True)
    draw.text((left + 90, top + 27), title, font=section_font, fill="white")

    return left, top + 130, right, bottom


# ============================================================
# 图像内容绘制
# ============================================================

def _draw_title(draw: ImageDraw.ImageDraw, today: date) -> None:
    font_title = art.load_font(118, bold=True)
    font_subtitle = art.load_font(62)

    _draw_center_text(
        draw,
        art.WIDTH // 2,
        45,
        f"北京时间：{today.strftime('%Y-%m-%d')}",
        font_title,
        art.INK,
    )
    _draw_center_text(
        draw,
        art.WIDTH // 2,
        188,
        "什么是基金？基金有哪些分类？",
        font_title,
        art.INK,
    )
    _draw_center_text(
        draw,
        art.WIDTH // 2,
        330,
        "基金小白第一课：先看懂本质，再看分类标签",
        font_subtitle,
        art.MUTED,
    )


def _draw_what_is_fund_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        430,
        720,
        "1｜基金到底是什么？",
    )

    summary_box = (left + 120, content_top + 55, right - 120, content_top + 215)
    art.rounded(draw, summary_box, 30, "#ffffff", art.LINE, 2)

    _draw_text_in_box(
        draw,
        (summary_box[0] + 55, summary_box[1] + 28, summary_box[2] - 55, summary_box[3] - 28),
        "一句话：基金就是很多人的钱集合起来，由基金公司按合同去投资，投资人买到的是基金份额。",
        font_size=62,
        min_font_size=46,
        bold=True,
        fill=art.INK,
        align="center",
        valign="center",
        line_gap=8,
    )

    card_y = content_top + 260
    card_h = section_bottom - card_y - 55
    card_gap = 70
    card_w = int((right - left - 240 - card_gap * 2) / 3)
    start_x = left + 120

    cards = [
        (
            "你买的是份额",
            art.BLUE,
            ["不是直接买一只股票", "份额数量 × 基金净值", "大致反映你的基金资产"],
        ),
        (
            "钱由基金公司管理",
            art.GOLD,
            ["基金经理或指数规则运作", "具体买什么看基金合同", "托管机构负责资产托管"],
        ),
        (
            "盈亏看净值变化",
            art.RED,
            ["净值上涨可能赚钱", "净值下跌可能亏损", "基金不是存款，不保本"],
        ),
    ]

    for idx, (title, color, bullets) in enumerate(cards):
        x = start_x + idx * (card_w + card_gap)

        art.rounded(draw, (x, card_y, x + card_w, card_y + card_h), 30, art.SOFT_CARD, art.LINE, 2)
        art.rounded(draw, (x + 34, card_y + 36, x + card_w - 34, card_y + 122), 28, color, None)

        _draw_text_in_box(
            draw,
            (x + 52, card_y + 40, x + card_w - 52, card_y + 118),
            title,
            font_size=60,
            min_font_size=44,
            bold=True,
            fill="white",
            align="center",
            valign="center",
        )

        _draw_bullets_in_box(
            draw,
            (x + 62, card_y + 165, x + card_w - 42, card_y + card_h - 42),
            bullets,
            color,
            font_size=52,
            min_font_size=40,
            gap=16,
            valign="center",
        )


def _draw_investment_type_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        1210,
        860,
        "2｜按主要投向，基金常见分为几类？",
    )

    card_gap = 48
    start_x = left + 110
    card_w = int((right - left - 220 - card_gap * 3) / 4)
    card_y = content_top + 60
    card_h = section_bottom - card_y - 145

    cards = [
        (
            "货币基金",
            art.GREEN,
            "主要投向现金类、短期工具\n波动通常较小\n常见于零钱管理场景",
        ),
        (
            "债券基金",
            art.BLUE,
            "主要投向债券\n波动通常高于货币基金\n也可能出现亏损",
        ),
        (
            "混合基金",
            art.GOLD,
            "股票、债券等都可能配置\n波动看实际仓位\n不同产品差异很大",
        ),
        (
            "股票基金",
            art.RED,
            "主要投向股票\n波动通常更大\n更考验风险承受能力",
        ),
    ]

    for idx, (title, color, body) in enumerate(cards):
        x = start_x + idx * (card_w + card_gap)

        art.rounded(draw, (x, card_y, x + card_w, card_y + card_h), 30, art.SOFT_CARD, art.LINE, 2)
        draw.rounded_rectangle((x, card_y, x + card_w, card_y + 20), radius=12, fill=color)

        _draw_text_in_box(
            draw,
            (x + 35, card_y + 44, x + card_w - 35, card_y + 118),
            title,
            font_size=58,
            min_font_size=42,
            bold=True,
            fill=color,
            align="center",
            valign="center",
        )

        _draw_text_in_box(
            draw,
            (x + 35, card_y + 142, x + card_w - 35, card_y + card_h - 35),
            body,
            font_size=48,
            min_font_size=36,
            bold=True,
            fill=art.INK,
            align="center",
            valign="center",
            line_gap=13,
        )

    risk_box = (left + 140, section_bottom - 112, right - 140, section_bottom - 38)
    art.rounded(draw, risk_box, 24, "#fff8eb", "#e8cf9e", 2)

    _draw_text_in_box(
        draw,
        (risk_box[0] + 35, risk_box[1] + 8, risk_box[2] - 35, risk_box[3] - 8),
        "简单理解：货币、债券、混合、股票，通常对应不同波动水平；但具体还要看基金持仓和合同。",
        font_size=56,
        min_font_size=42,
        bold=True,
        fill=art.INK,
        align="center",
        valign="center",
    )


def _draw_label_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        2140,
        760,
        "3｜常见基金标签，不要混着误解",
    )

    inner = (left + 120, content_top + 58, right - 120, section_bottom - 170)
    art.rounded(draw, inner, 30, "#ffffff", art.LINE, 2)

    card_gap = 48
    row_gap = 34
    card_w = int((inner[2] - inner[0] - 70 - card_gap * 1) / 2)
    card_h = 150
    start_x = inner[0] + 35
    start_y = inner[1] + 38

    labels = [
        (
            "主动基金",
            art.BLUE,
            "基金经理主动选择买什么",
        ),
        (
            "指数基金",
            art.GREEN,
            "主要跟踪某个指数",
        ),
        (
            "ETF / 场内基金",
            art.GOLD,
            "常在交易时间像股票一样买卖",
        ),
        (
            "QDII / 海外基金",
            art.RED,
            "投向海外市场，常有时差和汇率影响",
        ),
    ]

    for idx, (title, color, body) in enumerate(labels):
        row = idx // 2
        col = idx % 2
        x = start_x + col * (card_w + card_gap)
        y = start_y + row * (card_h + row_gap)

        art.rounded(draw, (x, y, x + card_w, y + card_h), 26, art.SOFT_CARD, art.LINE, 2)
        art.rounded(draw, (x + 28, y + 28, x + 230, y + card_h - 28), 24, color, None)

        _draw_text_in_box(
            draw,
            (x + 38, y + 34, x + 220, y + card_h - 34),
            title,
            font_size=46,
            min_font_size=34,
            bold=True,
            fill="white",
            align="center",
            valign="center",
        )

        _draw_text_in_box(
            draw,
            (x + 260, y + 28, x + card_w - 34, y + card_h - 28),
            body,
            font_size=50,
            min_font_size=38,
            bold=True,
            fill=art.INK,
            align="left",
            valign="center",
        )

    reminder = (left + 120, section_bottom - 132, right - 120, section_bottom - 42)
    art.rounded(draw, reminder, 26, "#fffdf8", "#e7d0a3", 2)

    _draw_text_in_box(
        draw,
        (reminder[0] + 45, reminder[1] + 10, reminder[2] - 45, reminder[3] - 10),
        "记住：分类可以重叠。一只基金可能同时是股票基金、指数基金、QDII基金和C类份额。",
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

    _draw_text_in_box(
        draw,
        (footer_box[0] + 45, footer_box[1] + 20, footer_box[2] - 45, footer_box[3] - 20),
        "仅供个人学习记录，不构成任何投资建议；基金不是存款，不保证收益。",
        font_size=72,
        min_font_size=50,
        bold=True,
        fill=art.RED,
        align="center",
        valign="center",
    )

    signature = "鱼师AHNS · 基金小白科普"
    sw, _ = art.text_size(draw, signature, art.FONT_SIGNATURE)
    draw.text((art.WIDTH - 150 - sw, top + 145), signature, font=art.FONT_SIGNATURE, fill="#7b8796")


def build_image(today: date) -> Image.Image:
    image = Image.new("RGBA", (art.WIDTH, art.HEIGHT), art.BG)

    _draw_strong_brand_watermarks(image)

    draw = ImageDraw.Draw(image)
    _draw_title(draw, today)
    _draw_what_is_fund_section(draw)
    _draw_investment_type_section(draw)
    _draw_label_section(draw)
    _draw_footer(draw)

    return image.convert("RGB")


# ============================================================
# 运行入口
# ============================================================

def run(today=None) -> bool:
    today_date = _normalize_today(today)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    image = build_image(today_date)
    image.save(OUTPUT_FILE, optimize=True, compress_level=9)

    print(f"基金基础科普图已生成: {OUTPUT_FILE.resolve()}")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成基金基础科普图：什么是基金，基金有哪些分类")
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