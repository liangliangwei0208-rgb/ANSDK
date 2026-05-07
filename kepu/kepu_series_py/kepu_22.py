"""
生成基金小白科普图：
基金持有 7 天怎么算？为什么 7 天内卖出费用高？

输出：
kepu/series/kepu_7day_holding_fee.png
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


OUTPUT_FILE = KEPU_DIR / "series" / "kepu_7day_holding_fee.png"


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
    font_title = art.load_font(116, bold=True)
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
        "基金持有 7 天怎么算？",
        font_title,
        art.INK,
    )
    _draw_center_text(
        draw,
        art.WIDTH // 2,
        330,
        "为什么 7 天内卖出，费用往往更高？",
        font_subtitle,
        art.MUTED,
    )


def _draw_core_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        430,
        760,
        "1｜先记结论：7 天到底怎么看？",
    )

    summary_box = (left + 120, content_top + 55, right - 120, content_top + 215)
    art.rounded(draw, summary_box, 30, "#ffffff", art.LINE, 2)

    _draw_text_in_box(
        draw,
        (summary_box[0] + 55, summary_box[1] + 28, summary_box[2] - 55, summary_box[3] - 28),
        "一句话：一般不是看下单日，而是看申购确认日到赎回确认日之间，满不满 7 个自然日。",
        font_size=64,
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
            "怎么算",
            art.BLUE,
            [
                "一般按自然日算",
                "不是按交易日算",
                "核心看两个确认日",
            ],
        ),
        (
            "7 天内",
            art.GOLD,
            [
                "持有期少于 7 天",
                "常见赎回费会更高",
                "很多基金常见档位是 1.5%",
            ],
        ),
        (
            "先看什么",
            art.GREEN,
            [
                "看基金合同和公告",
                "看招募说明书费率表",
                "不同基金规则可能不同",
            ],
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


def _draw_choice_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        1230,
        850,
        "2｜举个例子：这笔算不算 7 天内？",
    )

    card_gap = 48
    start_x = left + 110
    card_w = int((right - left - 220 - card_gap * 3) / 4)
    card_y = content_top + 60
    card_h = section_bottom - card_y - 145

    cards = [
        (
            "第 1 步",
            art.BLUE,
            "5月6日买入\n只是下单日\n不一定开始计时",
        ),
        (
            "第 2 步",
            art.GREEN,
            "5月7日确认\n申购确认日\n通常从这里算",
        ),
        (
            "第 3 步",
            art.GOLD,
            "5月13日赎回\n只是申请赎回\n还要看确认日",
        ),
        (
            "第 4 步",
            art.RED,
            "5月14日确认\n赎回确认日\n5月14日-5月7日=7天",
        ),
    ]

    for idx, (title, color, body) in enumerate(cards):
        x = start_x + idx * (card_w + card_gap)

        art.rounded(draw, (x, card_y, x + card_w, card_y + card_h), 30, art.SOFT_CARD, art.LINE, 2)
        draw.rounded_rectangle((x, card_y, x + card_w, card_y + 20), radius=12, fill=color)

        _draw_text_in_box(
            draw,
            (x + 30, card_y + 42, x + card_w - 30, card_y + 126),
            title,
            font_size=52,
            min_font_size=38,
            bold=True,
            fill=color,
            align="center",
            valign="center",
        )

        _draw_text_in_box(
            draw,
            (x + 35, card_y + 150, x + card_w - 35, card_y + card_h - 35),
            body,
            font_size=46,
            min_font_size=34,
            bold=True,
            fill=art.INK,
            align="center",
            valign="center",
            line_gap=12,
        )

    note_box = (left + 140, section_bottom - 112, right - 140, section_bottom - 38)
    art.rounded(draw, note_box, 24, "#fff8eb", "#e8cf9e", 2)

    _draw_text_in_box(
        draw,
        (note_box[0] + 35, note_box[1] + 8, note_box[2] - 35, note_box[3] - 8),
        "所以：这笔通常不属于“7 天内卖出”；但如果更早赎回，就可能仍在 7 天内。",
        font_size=58,
        min_font_size=42,
        bold=True,
        fill=art.INK,
        align="center",
        valign="center",
    )


def _draw_mistakes_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        2130,
        770,
        "3｜小白最容易误解什么？",
    )

    inner = (left + 120, content_top + 60, right - 120, section_bottom - 170)
    art.rounded(draw, inner, 30, "#ffffff", art.LINE, 2)

    bullets = [
        "误区一：按买入当天开始算。实际很多时候要从“申购确认日”开始算。",
        "误区二：按交易日算。实际常见口径是按自然日算，周末也可能计入。",
        "误区三：满 7 天就是第 7 天随便卖。还要注意赎回申请日和赎回确认日可能不同。",
        "误区四：所有基金都一样。货币基金、ETF、持有期基金等规则可能不一样。",
    ]

    _draw_bullets_in_box(
        draw,
        (inner[0] + 80, inner[1] + 45, inner[2] - 70, inner[3] - 42),
        bullets,
        art.RED,
        font_size=54,
        min_font_size=40,
        gap=18,
        valign="center",
    )

    reminder = (left + 120, section_bottom - 132, right - 120, section_bottom - 42)
    art.rounded(draw, reminder, 26, "#fffdf8", "#e7d0a3", 2)

    _draw_text_in_box(
        draw,
        (reminder[0] + 45, reminder[1] + 10, reminder[2] - 45, reminder[3] - 10),
        "简单记：不是看你买了几天，而是看“确认后持有了几天”。",
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
        "仅供个人学习记录，不构成任何投资建议；具体持有期与赎回费请以基金公告、合同和销售平台展示为准。",
        font_size=66,
        min_font_size=46,
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
    _draw_core_section(draw)
    _draw_choice_section(draw)
    _draw_mistakes_section(draw)
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

    print(f"基金持有7天科普图已生成: {OUTPUT_FILE.resolve()}")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成基金科普图：基金持有 7 天怎么算")
    parser.add_argument(
        "--today",
        default=None,
        help="用于测试的北京时间日期，例如 2026-05-05；默认使用今天。",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(today=args.today)


if __name__ == "__main__":
    main()