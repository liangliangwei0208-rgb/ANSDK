"""
生成基金小白科普图：
今天买基金，按哪天净值算？

输出：
kepu/series/kepu_buy_nav_day.png
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


OUTPUT_FILE = KEPU_DIR / "series" / "kepu_buy_nav_day.png"


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
    在指定区域内绘制文本，并抵消字体 bbox 偏移，保证视觉居中。
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
        "今天买基金，按哪天净值算？",
        font_title,
        art.INK,
    )
    _draw_center_text(
        draw,
        art.WIDTH // 2,
        330,
        "先看今天是不是交易日，再看是不是 15:00 前提交",
        font_subtitle,
        art.MUTED,
    )


def _draw_two_questions_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        430,
        750,
        "1｜今天买，先看两个问题",
    )

    summary_box = (left + 120, content_top + 55, right - 120, content_top + 215)
    art.rounded(draw, summary_box, 30, "#ffffff", art.LINE, 2)

    _draw_text_in_box(
        draw,
        (summary_box[0] + 55, summary_box[1] + 28, summary_box[2] - 55, summary_box[3] - 28),
        "一句话：今天买基金，不一定按今天净值算，关键看“交易日”和“15:00”。",
        font_size=66,
        min_font_size=48,
        bold=True,
        fill=art.INK,
        align="center",
        valign="center",
        line_gap=8,
    )

    card_y = content_top + 260
    card_h = section_bottom - card_y - 55
    card_gap = 80
    card_w = int((right - left - 240 - card_gap) / 2)
    start_x = left + 120

    cards = [
        (
            "问题 1",
            art.BLUE,
            "今天是不是交易日？",
            [
                "交易日：通常是股市开市日",
                "周末、节假日通常顺延",
                "特殊基金还要看基金公告",
            ],
        ),
        (
            "问题 2",
            art.GOLD,
            "是不是 15:00 前提交？",
            [
                "15:00 前通常算当天申请",
                "15:00 后通常算下一交易日",
                "提交时还不知道最终净值",
            ],
        ),
    ]

    for idx, (label, color, title, bullets) in enumerate(cards):
        x = start_x + idx * (card_w + card_gap)

        art.rounded(draw, (x, card_y, x + card_w, card_y + card_h), 32, art.SOFT_CARD, art.LINE, 2)

        art.rounded(draw, (x + 42, card_y + 38, x + 260, card_y + 128), 30, color, None)
        _draw_text_in_box(
            draw,
            (x + 56, card_y + 42, x + 246, card_y + 124),
            label,
            font_size=58,
            min_font_size=42,
            bold=True,
            fill="white",
            align="center",
            valign="center",
        )

        _draw_text_in_box(
            draw,
            (x + 300, card_y + 38, x + card_w - 50, card_y + 128),
            title,
            font_size=62,
            min_font_size=46,
            bold=True,
            fill=color,
            align="left",
            valign="center",
        )

        _draw_bullets_in_box(
            draw,
            (x + 82, card_y + 175, x + card_w - 60, card_y + card_h - 48),
            bullets,
            color,
            font_size=58,
            min_font_size=44,
            gap=22,
            valign="center",
        )


def _draw_scenarios_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        1230,
        840,
        "2｜三个常见场景怎么判断？",
    )

    start_x = left + 120
    card_gap = 70
    card_w = int((right - left - 240 - card_gap * 2) / 3)
    card_y = content_top + 62
    card_h = section_bottom - card_y - 150

    cards = [
        (
            "交易日 15:00 前买",
            art.BLUE,
            "通常按当天净值",
            [
                "例如周一 14:30 买",
                "通常按周一净值确认",
                "周一净值晚上或之后公布",
            ],
        ),
        (
            "交易日 15:00 后买",
            art.GOLD,
            "通常按下一交易日净值",
            [
                "例如周一 15:30 买",
                "通常按周二净值确认",
                "不是按周一净值买",
            ],
        ),
        (
            "周末 / 节假日买",
            art.GREEN,
            "通常顺延到下一交易日",
            [
                "例如周六买基金",
                "通常按下周一申请处理",
                "再按对应交易日净值确认",
            ],
        ),
    ]

    for idx, (title, color, tag, bullets) in enumerate(cards):
        x = start_x + idx * (card_w + card_gap)

        art.rounded(draw, (x, card_y, x + card_w, card_y + card_h), 30, art.SOFT_CARD, art.LINE, 2)

        art.rounded(draw, (x + 34, card_y + 36, x + card_w - 34, card_y + 126), 28, color, None)
        _draw_text_in_box(
            draw,
            (x + 52, card_y + 40, x + card_w - 52, card_y + 122),
            title,
            font_size=56,
            min_font_size=40,
            bold=True,
            fill="white",
            align="center",
            valign="center",
        )

        _draw_text_in_box(
            draw,
            (x + 58, card_y + 150, x + card_w - 58, card_y + 224),
            tag,
            font_size=58,
            min_font_size=42,
            bold=True,
            fill=color,
            align="center",
            valign="center",
        )

        _draw_bullets_in_box(
            draw,
            (x + 60, card_y + 250, x + card_w - 42, card_y + card_h - 42),
            bullets,
            color,
            font_size=52,
            min_font_size=40,
            gap=16,
            valign="center",
        )

    note_box = (left + 140, section_bottom - 112, right - 140, section_bottom - 38)
    art.rounded(draw, note_box, 24, "#fff8eb", "#e8cf9e", 2)

    _draw_text_in_box(
        draw,
        (note_box[0] + 35, note_box[1] + 8, note_box[2] - 35, note_box[3] - 8),
        "注意：这里讲的是常见场外开放式基金；货币基金、QDII、场内 ETF 等规则可能不同。",
        font_size=58,
        min_font_size=42,
        bold=True,
        fill=art.INK,
        align="center",
        valign="center",
    )


def _draw_misunderstanding_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        2120,
        780,
        "3｜小白最容易搞错什么？",
    )

    inner = (left + 120, content_top + 60, right - 120, section_bottom - 170)
    art.rounded(draw, inner, 30, "#ffffff", art.LINE, 2)

    bullets = [
        "按哪天净值算，不等于当天马上知道成交价格。",
        "确认净值，不等于马上看到份额；份额确认通常还要等平台处理。",
        "买入确认，不等于马上有收益；收益从哪天算也要看基金类型。",
        "不要把“今天买”理解成“按此刻价格买”，基金通常不是实时成交价格。",
    ]

    _draw_bullets_in_box(
        draw,
        (inner[0] + 80, inner[1] + 45, inner[2] - 70, inner[3] - 42),
        bullets,
        art.RED,
        font_size=58,
        min_font_size=44,
        gap=20,
        valign="center",
    )

    reminder = (left + 120, section_bottom - 132, right - 120, section_bottom - 42)
    art.rounded(draw, reminder, 26, "#fffdf8", "#e7d0a3", 2)

    _draw_text_in_box(
        draw,
        (reminder[0] + 45, reminder[1] + 10, reminder[2] - 45, reminder[3] - 10),
        "简单记：交易日 15:00 前看当天；15:00 后或非交易日，看下一交易日。",
        font_size=62,
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
        "仅供个人学习记录，不构成任何投资建议；具体规则以基金公告和销售平台展示为准。",
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
    _draw_two_questions_section(draw)
    _draw_scenarios_section(draw)
    _draw_misunderstanding_section(draw)
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

    print(f"基金买入净值日期科普图已生成: {OUTPUT_FILE.resolve()}")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成基金科普图：今天买基金按哪天净值算")
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