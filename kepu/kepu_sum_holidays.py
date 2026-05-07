"""
生成节后海外基金净值补更新规则科普图。

本脚本只在节后第 1 / 第 2 个 A 股交易日生成图片；普通周末、节假日休市日、
节后第 3 个交易日起都只打印原因并正常退出。

输出：
output/kepu_sum_holidays.png
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


KEPU_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = KEPU_DIR.parent

for import_path in (PROJECT_ROOT, KEPU_DIR):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

import first_pic as art
from sum_holidays import PostHolidayContext, detect_post_holiday_context


OUTPUT_FILE = PROJECT_ROOT / "output" / "kepu_sum_holidays.png"


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
    """
    精准居中绘制单行文字。

    Pillow 的 textbbox 可能存在 bbox[0] / bbox[1] 偏移。
    这里抵消 bbox 偏移，使实际字形外框按视觉中心对齐。
    """
    bbox = _text_bbox(draw, text, font)
    w = bbox[2] - bbox[0]
    draw_x = center_x - w / 2 - bbox[0]
    draw_y = y - bbox[1]
    draw.text((draw_x, draw_y), text, font=font, fill=fill)


def _wrap_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
) -> list[str]:
    """
    按像素宽度折行。
    中文按字符处理；显式换行会被保留。
    """
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
    boxes: list[tuple[int, int, int, int]] = []

    if not lines:
        return 0, [], []

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
    在指定区域内绘制文本，并做精准视觉居中。
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
        height, lines, boxes = _measure_wrapped_text(draw, text, font, max_width, line_gap)

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
    text_width = max(10, max_width - 48)

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
    """
    在指定区域内绘制项目符号，并做整体垂直居中。
    """
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
# 水印与公共绘图
# ============================================================

def _draw_strong_brand_watermarks(image: Image.Image) -> None:
    """
    更明显的全页品牌水印：
    - 只显示“鱼师AHNS”
    - 字号更大、透明度更高、斜向平铺
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
    """
    与 first_pic.draw_section_shell 保持同结构，只略微放大标题字号。
    """
    left, right = 150, art.WIDTH - 150
    bottom = top + height

    art.rounded(draw, (left, top, right, bottom), 38, art.CARD_BG, art.LINE, 2)
    draw.rounded_rectangle((left, top, right, top + 130), radius=38, fill=art.NAVY)
    draw.rectangle((left, top + 72, right, top + 130), fill=art.NAVY)

    section_font = art.load_font(66, bold=True)
    draw.text((left + 90, top + 27), title, font=section_font, fill="white")

    return left, top + 130, right, bottom


# ============================================================
# 图像绘制
# ============================================================

def _draw_title(draw: ImageDraw.ImageDraw, context: PostHolidayContext) -> None:
    font_title = art.load_font(124, bold=True)
    font_subtitle = art.load_font(62)

    _draw_center_text(
        draw,
        art.WIDTH // 2,
        45,
        f"北京时间：{context.today.strftime('%Y-%m-%d')}",
        font_title,
        art.INK,
    )
    _draw_center_text(
        draw,
        art.WIDTH // 2,
        188,
        "节后海外基金预估收益率怎么算？",
        font_title,
        art.INK,
    )
    _draw_center_text(
        draw,
        art.WIDTH // 2,
        330,
        "说明模型区间观察口径，不代表基金公司净值公告",
        font_subtitle,
        art.MUTED,
    )


def _draw_timeline_section(draw: ImageDraw.ImageDraw, context: PostHolidayContext) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        430,
        735,
        "1｜节后为什么会出现“补更新”？",
    )

    box_w, box_h = 620, 180
    gap = 165
    box_y = content_top + 56
    xs = [
        left + 150,
        left + 150 + box_w + gap,
        left + 150 + (box_w + gap) * 2,
    ]

    items = [
        ("节前", "最后一个海外估值日，记作 T日", art.BLUE),
        ("假期中", "海外可能交易，国内披露会暂停", art.GOLD),
        ("节后", "把缺口分批补更新", art.GREEN),
    ]

    for idx, (title, sub, color) in enumerate(items):
        x = xs[idx]

        art.rounded(draw, (x, box_y, x + box_w, box_y + box_h), 28, art.SOFT_CARD, art.LINE, 2)
        draw.rounded_rectangle((x, box_y, x + box_w, box_y + 18), radius=12, fill=color)

        _draw_text_in_box(
            draw,
            (x + 35, box_y + 32, x + box_w - 35, box_y + box_h - 16),
            f"{title}\n{sub}",
            font_size=64,
            min_font_size=44,
            bold=True,
            fill=art.INK,
            align="center",
            valign="center",
            line_gap=12,
        )

        if idx < len(items) - 1:
            art.draw_arrow(draw, x + box_w + 42, box_y + box_h // 2, xs[idx + 1] - 45)

    note_box = (left + 120, box_y + box_h + 58, right - 120, section_bottom - 55)
    art.rounded(draw, note_box, 28, "#ffffff", art.LINE, 2)

    bullets = [
        "海外基金净值通常不是海外市场一收盘就立刻公布，中间还要等估值和公告。",
        "国内放假时，海外市场可能照常交易；但国内平台可能等节后再集中显示。",
        "所以节后看到的涨跌幅，可能是在补前几天的估值影响，不一定是当天市场涨跌。",
    ]

    _draw_bullets_in_box(
        draw,
        (note_box[0] + 70, note_box[1] + 34, note_box[2] - 60, note_box[3] - 34),
        bullets,
        art.BLUE,
        font_size=64,
        min_font_size=48,
        gap=14,
        valign="center",
    )


def _draw_rule_section(draw: ImageDraw.ImageDraw, context: PostHolidayContext) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        1235,
        880,
        "2｜节后第1天、第2天收益率计算方式",
    )

    card_gap = 70
    card_w = int((right - left - 240 - card_gap * 2) / 3)
    card_h = section_bottom - content_top - 135
    card_y = content_top + 65
    start_x = left + 120

    cards = [
        (
            "节后第 1 天",
            art.BLUE,
            "单日观察",
            [
                "看 T日 的预估收益率",
                "T日=节前最后一个海外估值日",
                "这一天不做累计",
            ],
        ),
        (
            "节后第 2 天",
            art.GOLD,
            "区间累计",
            [
                "从 T日 之后开始看",
                "只累计实际存在的海外估值日",
                "周末或海外休市没有数据就跳过",
            ],
        ),
        (
            "节后第 3 天起",
            art.GREEN,
            "回到日常节奏",
            [
                "不再单独做节后补更新图",
                "继续看普通每日模型观察",
                "最终仍以基金公告为准",
            ],
        ),
    ]

    for idx, (title, color, tag, bullets) in enumerate(cards):
        x = start_x + idx * (card_w + card_gap)

        art.rounded(draw, (x, card_y, x + card_w, card_y + card_h), 30, art.SOFT_CARD, art.LINE, 2)

        art.rounded(draw, (x + 34, card_y + 36, x + card_w - 34, card_y + 128), 28, color, None)
        _draw_text_in_box(
            draw,
            (x + 52, card_y + 38, x + card_w - 52, card_y + 124),
            title,
            font_size=64,
            min_font_size=46,
            bold=True,
            fill="white",
            align="center",
            valign="center",
        )

        _draw_text_in_box(
            draw,
            (x + 58, card_y + 152, x + card_w - 58, card_y + 224),
            tag,
            font_size=62,
            min_font_size=46,
            bold=True,
            fill=color,
            align="center",
            valign="center",
        )

        _draw_bullets_in_box(
            draw,
            (x + 58, card_y + 252, x + card_w - 44, card_y + card_h - 55),
            bullets,
            color,
            font_size=59,
            min_font_size=43,
            gap=22,
            valign="center",
        )

    badge_box = (left + 140, section_bottom - 112, right - 140, section_bottom - 38)
    art.rounded(draw, badge_box, 24, "#fff8eb", "#e8cf9e", 2)

    badge_text = "简单记：第1天看 T日 单日；第2天看 T日之后的有效估值日累计；第3天回到日常观察。"

    _draw_text_in_box(
        draw,
        (badge_box[0] + 35, badge_box[1] + 8, badge_box[2] - 35, badge_box[3] - 8),
        badge_text,
        font_size=60,
        min_font_size=42,
        bold=True,
        fill=art.INK,
        align="center",
        valign="center",
    )


def _draw_boundary_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = _draw_section_shell(
        draw,
        2200,
        735,
        "3｜怎么看这张图",
    )

    inner = (left + 120, content_top + 62, right - 120, section_bottom - 160)
    art.rounded(draw, inner, 30, "#ffffff", art.LINE, 2)

    bullets = [
        "这是个人模型观察，用来帮助理解节后可能补披露的是哪一段。",
        "第2天的“累计”不是把自然日直接相加，而是按实际有估值的日期连续观察。",
        "它不是实时净值，也不是基金公司公告；最终净值和披露日期以公告为准。",
    ]

    _draw_bullets_in_box(
        draw,
        (inner[0] + 80, inner[1] + 48, inner[2] - 70, inner[3] - 42),
        bullets,
        art.RED,
        font_size=64,
        min_font_size=48,
        gap=22,
        valign="center",
    )

    footer = (left + 120, section_bottom - 130, right - 120, section_bottom - 42)
    art.rounded(draw, footer, 26, "#fffdf8", "#e7d0a3", 2)

    _draw_text_in_box(
        draw,
        (footer[0] + 35, footer[1] + 8, footer[2] - 35, footer[3] - 8),
        "仅供个人学习记录，不构成任何投资建议；不代表基金公司公告。",
        font_size=62,
        min_font_size=44,
        bold=True,
        fill=art.RED,
        align="center",
        valign="center",
    )


def build_image(context: PostHolidayContext) -> Image.Image:
    image = Image.new("RGBA", (art.WIDTH, art.HEIGHT), art.BG)

    _draw_strong_brand_watermarks(image)

    draw = ImageDraw.Draw(image)
    _draw_title(draw, context)
    _draw_timeline_section(draw, context)
    _draw_rule_section(draw, context)
    _draw_boundary_section(draw)

    return image.convert("RGB")


def run(today: str | None = None) -> bool:
    context = detect_post_holiday_context(today=today)
    print(context.reason)

    if not context.should_generate:
        return False

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    image = build_image(context)
    image.save(OUTPUT_FILE, optimize=True, compress_level=9)

    print(f"节后海外基金补更新科普图已生成: {OUTPUT_FILE.resolve()}")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成节后海外基金净值补更新规则科普图")
    parser.add_argument(
        "--today",
        default=None,
        help="用于测试的北京时间日期，例如 2026-05-06；默认使用今天。",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(today=args.today)


if __name__ == "__main__":
    main()