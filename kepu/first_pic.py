"""
kepu/first_pic.py

用 Pillow 生成抖音图集第一张说明图：
output/first_pic.png

本脚本只负责绘制固定说明图，不读取行情、不访问网络、不依赖缓存。
"""

from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WIDTH = 2720
HEIGHT = 3200
OUTPUT_FILE = PROJECT_ROOT / "output" / "first_pic.png"

BG = "#f4f7fb"
NAVY = "#2f3f5c"
INK = "#111827"
MUTED = "#667085"
LINE = "#d8e0ec"
CARD_BG = "#ffffff"
SOFT_CARD = "#f6f8fb"
BLUE = "#2f65a7"
GOLD = "#d5a035"
GREEN = "#2b855f"
RED = "#be3b3b"


@lru_cache(maxsize=None)
def load_font(size: int, *, bold: bool = False) -> ImageFont.ImageFont:
    """
    跨平台加载中文字体。

    修复重点：
    Linux 下 NotoSansCJK-Regular.ttc / NotoSansCJK-Bold.ttc 默认第 0 个 face
    很可能是 JP 日文字形，会导致“复”等汉字显示不符合简体中文习惯。

    因此：
    1. 优先使用明确的 SC 简体中文字体文件；
    2. 对 Noto CJK TTC 集合优先尝试 index=2；
    3. 对 TTC 集合校验字体名是否包含 SC；
    4. 找不到中文字体时不再静默 fallback，而是直接报错。
    """

    def try_load(path_str: str, index: int | None = None) -> ImageFont.ImageFont | None:
        path = Path(path_str)
        if not path.exists():
            return None

        try:
            if index is None:
                return ImageFont.truetype(str(path), size=size)
            return ImageFont.truetype(str(path), size=size, index=index)
        except Exception:
            return None

    def font_name(font: ImageFont.ImageFont) -> str:
        try:
            name = font.getname()
            return " ".join(str(x) for x in name)
        except Exception:
            return ""

    if bold:
        direct_candidates = [
            # Windows
            r"C:\Windows\Fonts\msyhbd.ttc",
            r"C:\Windows\Fonts\simhei.ttf",
            r"C:\Windows\Fonts\msyh.ttc",

            # Linux
            "/usr/share/fonts/opentype/noto/NotoSansCJKsc-Bold.otf",
            "/usr/share/fonts/opentype/noto/NotoSerifCJKsc-Bold.otf",
            "/usr/share/fonts/truetype/noto/NotoSansSC-Bold.ttf",
            "/usr/share/fonts/truetype/noto/NotoSerifSC-Bold.ttf",

            # Linux 其他常见中文字体
            "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",

            # macOS
            "/System/Library/Fonts/PingFang.ttc",
            "/System/Library/Fonts/STHeiti Medium.ttc",
        ]

        ttc_candidates = [
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
            "/usr/share/fonts/opentype/noto/NotoSerifCJK-Bold.ttc",
        ]
    else:
        direct_candidates = [
            # Windows
            r"C:\Windows\Fonts\msyh.ttc",
            r"C:\Windows\Fonts\simhei.ttf",
            r"C:\Windows\Fonts\simsun.ttc",

            # Linux
            "/usr/share/fonts/opentype/noto/NotoSansCJKsc-Regular.otf",
            "/usr/share/fonts/opentype/noto/NotoSerifCJKsc-Regular.otf",
            "/usr/share/fonts/truetype/noto/NotoSansSC-Regular.ttf",
            "/usr/share/fonts/truetype/noto/NotoSerifSC-Regular.ttf",

            # Linux 其他常见中文字体
            "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",

            # macOS
            "/System/Library/Fonts/PingFang.ttc",
            "/System/Library/Fonts/STHeiti Light.ttc",
        ]

        ttc_candidates = [
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc",
        ]

    # 先尝试明确的中文字体路径
    for font_path in direct_candidates:
        font = try_load(font_path)
        if font is not None:
            return font

    # 再尝试 Noto CJK TTC 集合。index=2 通常是 SC。
    # 同时做字体名校验，避免误用 JP/KR/TC/HK。
    for font_path in ttc_candidates:
        for idx in [2, 0, 1, 3, 4, 5, 6, 7, 8, 9]:
            font = try_load(font_path, index=idx)
            if font is None:
                continue

            name = font_name(font)
            if "CJK SC" in name or "Sans SC" in name or "Serif SC" in name or name.endswith(" SC"):
                return font

    raise RuntimeError(
        "未找到可用的简体中文字体。\n"
        "Linux 可先安装：sudo apt install -y fonts-noto-cjk fonts-noto-cjk-extra\n"
        "然后执行：fc-cache -fv\n"
        "如果仍失败，请把 NotoSansCJKsc / NotoSansSC 字体路径加入 load_font()。"
    )


# 全局字号：在原版基础上略微放大，适配抖音图文阅读
FONT_TITLE = load_font(118, bold=True)
FONT_SUBTITLE = load_font(58)
FONT_TIME = load_font(50, bold=True)
FONT_SECTION = load_font(64, bold=True)
FONT_BOX_TITLE = load_font(60, bold=True)
FONT_BOX_SUB = load_font(50)
FONT_BODY = load_font(52, bold=True)
FONT_SMALL = load_font(44)
FONT_CARD_TITLE = load_font(54, bold=True)
FONT_CARD_BODY = load_font(50, bold=True)
FONT_FOOTER = load_font(88, bold=True)
FONT_SIGNATURE = load_font(38)


def text_bbox(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> tuple[int, int, int, int]:
    return draw.textbbox((0, 0), text, font=font)


def text_size(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> tuple[int, int]:
    bbox = text_bbox(draw, text, font)
    return int(bbox[2] - bbox[0]), int(bbox[3] - bbox[1])


def draw_center_text(
    draw: ImageDraw.ImageDraw,
    center_x: int,
    y: int,
    text: str,
    font: ImageFont.ImageFont,
    fill: str,
) -> None:
    bbox = text_bbox(draw, text, font)
    w = bbox[2] - bbox[0]
    x = center_x - w / 2 - bbox[0]
    draw.text((x, y - bbox[1]), text, font=font, fill=fill)


def wrap_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
) -> list[str]:
    """按像素宽度折行，中文按字符处理。保留换行。"""
    paragraphs = text.split("\n")
    lines: list[str] = []

    for para in paragraphs:
        if para == "":
            lines.append("")
            continue

        current = ""
        for char in para:
            candidate = current + char
            if text_size(draw, candidate, font)[0] <= max_width:
                current = candidate
            else:
                if current:
                    lines.append(current)
                current = char

        if current:
            lines.append(current)

    return lines


def draw_wrapped_text(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    text: str,
    font: ImageFont.ImageFont,
    fill: str,
    max_width: int,
    line_gap: int = 10,
    align: str = "left",
) -> int:
    x, y = xy
    lines = wrap_text(draw, text, font, max_width)

    for line in lines:
        bbox = text_bbox(draw, line, font)
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]

        if align == "center":
            tx = x + (max_width - w) / 2 - bbox[0]
        elif align == "right":
            tx = x + (max_width - w) - bbox[0]
        else:
            tx = x - bbox[0]

        draw.text((tx, y - bbox[1]), line, font=font, fill=fill)
        y += h + line_gap

    return y


def measure_wrapped_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
    line_gap: int = 10,
) -> tuple[int, list[str], list[tuple[int, int, int, int]]]:
    lines = wrap_text(draw, text, font, max_width)
    if not lines:
        return 0, [], []

    boxes: list[tuple[int, int, int, int]] = []
    height = 0
    for idx, line in enumerate(lines):
        bbox = text_bbox(draw, line, font)
        boxes.append(bbox)
        line_h = bbox[3] - bbox[1]
        height += line_h
        if idx < len(lines) - 1:
            height += line_gap

    return height, lines, boxes


def draw_text_in_box(
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
    在指定区域内自适应字号和换行，避免说明文字跑出卡片。

    关键修复：
    Pillow 的 textbbox((0, 0), text, font=font) 往往不是从 y=0 开始，
    特别是 Linux + Noto CJK 字体下，bbox[1] 偏移更明显。
    如果只按 bbox 高度做垂直居中，再直接 draw.text((x, y), ...)，
    视觉上会整体下沉，不是真正的上下居中。

    本函数绘制时会抵消 bbox[0] / bbox[1]，使“实际字形外框”严格贴合居中区域。
    """
    left, top, right, bottom = box
    max_width = max(10, right - left)
    max_height = max(10, bottom - top)

    chosen_font = load_font(font_size, bold=bold)
    chosen_lines: list[str] = []
    chosen_boxes: list[tuple[int, int, int, int]] = []
    chosen_height = 0

    for size in range(font_size, min_font_size - 1, -2):
        font = load_font(size, bold=bold)
        height, lines, boxes = measure_wrapped_text(draw, text, font, max_width, line_gap)
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


def measure_bullets(
    draw: ImageDraw.ImageDraw,
    bullets: list[str],
    font: ImageFont.ImageFont,
    max_width: int,
    line_gap: int,
    gap: int,
) -> tuple[int, list[list[str]], list[list[tuple[int, int, int, int]]]]:
    wrapped: list[list[str]] = []
    wrapped_boxes: list[list[tuple[int, int, int, int]]] = []
    height = 0
    text_width = max(10, max_width - 48)

    for idx, text in enumerate(bullets):
        lines = wrap_text(draw, text, font, text_width)
        boxes = [text_bbox(draw, line, font) for line in lines]
        wrapped.append(lines)
        wrapped_boxes.append(boxes)

        bullet_height = 0
        for line_idx, bbox in enumerate(boxes):
            line_h = bbox[3] - bbox[1]
            bullet_height += line_h
            if line_idx < len(lines) - 1:
                bullet_height += line_gap

        height += bullet_height
        if idx < len(bullets) - 1:
            height += gap

    return height, wrapped, wrapped_boxes


def draw_bullets_in_box(
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
    bullets: list[str],
    dot_color: str,
    *,
    font_size: int,
    min_font_size: int = 30,
    bold: bool = True,
    fill: str = INK,
    line_gap: int = 6,
    gap: int = 16,
    valign: str = "top",
) -> None:
    """在指定内容框内绘制项目符号；字号会随可用高度自动收缩。"""
    left, top, right, bottom = box
    max_width = max(10, right - left)
    max_height = max(10, bottom - top)

    chosen_font = load_font(font_size, bold=bold)
    chosen_wrapped: list[list[str]] = []
    chosen_wrapped_boxes: list[list[tuple[int, int, int, int]]] = []
    chosen_height = 0

    for size in range(font_size, min_font_size - 1, -2):
        font = load_font(size, bold=bold)
        height, wrapped, wrapped_boxes = measure_bullets(draw, bullets, font, max_width, line_gap, gap)
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
        dot_y = y + 12
        draw.ellipse((left, dot_y, left + 18, dot_y + 18), fill="#dbeafe")
        draw.ellipse((left + 5, dot_y + 5, left + 13, dot_y + 13), fill=dot_color)

        line_y = y
        for line, bbox in zip(bullet_lines, bullet_boxes):
            line_h = bbox[3] - bbox[1]
            draw.text((left + 48 - bbox[0], line_y - bbox[1]), line, font=chosen_font, fill=fill)
            line_y += line_h + line_gap
            if line_y > bottom:
                return

        y = line_y - line_gap + gap
        if y > bottom:
            return


def rounded(
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
    radius: int,
    fill: str,
    outline: str | None = None,
    width: int = 1,
) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def draw_arrow(draw: ImageDraw.ImageDraw, x1: int, y: int, x2: int, color: str = LINE) -> None:
    draw.line((x1, y, x2 - 26, y), fill=color, width=8)
    draw.polygon([(x2 - 26, y - 20), (x2 - 26, y + 20), (x2 + 8, y)], fill=color)


def draw_watermarks(image: Image.Image) -> None:
    """
    更明显的品牌水印：
    - 只显示“鱼师AHNS”
    - 比原版更大、更深、更密
    - 斜向平铺，适合抖音图文传播时保留品牌识别
    """
    overlay = Image.new("RGBA", image.size, (255, 255, 255, 0))

    text = "鱼师AHNS"
    font = load_font(150, bold=True)
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

    for row, y in enumerate(range(280, HEIGHT - 80, step_y)):
        offset_x = 0 if row % 2 == 0 else 320
        for x in range(-260 + offset_x, WIDTH + 260, step_x):
            overlay.alpha_composite(rotated, (x, y))

    image.alpha_composite(overlay)


def draw_title(draw: ImageDraw.ImageDraw) -> None:
    time_text = "北京时间：" + datetime.now(
        ZoneInfo("Asia/Shanghai")
    ).strftime("%Y-%m-%d %H:%M")

    draw_center_text(draw, WIDTH // 2, 45, time_text, FONT_TITLE, INK)
    draw_center_text(draw, WIDTH // 2, 185, "基金预估图怎么看？", FONT_TITLE, INK)
    draw_center_text(draw, WIDTH // 2, 325, "个人公开数据建模复盘，不是净值预告", FONT_SUBTITLE, MUTED)


def draw_section_shell(
    draw: ImageDraw.ImageDraw,
    top: int,
    height: int,
    title: str,
) -> tuple[int, int, int, int]:
    left, right = 150, WIDTH - 150
    bottom = top + height

    rounded(draw, (left, top, right, bottom), 38, CARD_BG, LINE, 2)
    draw.rounded_rectangle((left, top, right, top + 130), radius=38, fill=NAVY)
    draw.rectangle((left, top + 72, right, top + 130), fill=NAVY)
    draw.text((left + 90, top + 29), title, font=FONT_SECTION, fill="white")

    return left, top + 130, right, bottom


def draw_top_time_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = draw_section_shell(
        draw,
        440,
        720,
        "1｜海外基金估值时间点为什么不同？",
    )

    box_w, box_h = 650, 155
    box_y = content_top + 42
    xs = [left + 120, left + 885, left + 1650]
    items = [
        ("北京时间发图日", "你看到图片的日期"),
        ("海外市场交易日", "常参考最近有效收盘"),
        ("基金公告净值日", "基金公司最终披露"),
    ]

    for x, (title, sub) in zip(xs, items):
        rounded(draw, (x, box_y, x + box_w, box_y + box_h), 26, SOFT_CARD, LINE, 2)

        draw_text_in_box(
            draw,
            (x + 28, box_y + 28, x + box_w - 28, box_y + 90),
            title,
            font_size=60,
            min_font_size=46,
            bold=True,
            fill=INK,
            align="center",
            valign="center",
        )
        draw_text_in_box(
            draw,
            (x + 28, box_y + 91, x + box_w - 28, box_y + 144),
            sub,
            font_size=50,
            min_font_size=38,
            bold=False,
            fill=MUTED,
            align="center",
            valign="center",
        )

    arrow_y = box_y + box_h // 2
    draw_arrow(draw, xs[0] + box_w + 40, arrow_y, xs[1] - 35)
    draw_arrow(draw, xs[1] + box_w + 40, arrow_y, xs[2] - 35)

    bullet_top = box_y + box_h + 45
    bullet_box = (left + 110, bullet_top, right - 110, section_bottom - 55)
    rounded(draw, bullet_box, 26, "#ffffff", LINE, 2)

    bullets = [
        "海外/QDII基金受境外市场收盘、汇率和基金公司公告节奏影响",
        "北京时间当天看到的观察值，可能对应海外市场上一交易日或最近有效交易日",
        "节假日、周末、海外或国内市场休市时，估值日期可能不同步",
    ]

    draw_bullets_in_box(
        draw,
        (bullet_box[0] + 70, bullet_box[1] + 32, bullet_box[2] - 50, bullet_box[3] - 32),
        bullets,
        BLUE,
        font_size=54,
        min_font_size=42,
        gap=12,
        valign="center",
    )


def draw_bullets(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    bullets: list[str],
    dot_color: str,
    font: ImageFont.ImageFont,
    max_width: int,
    gap: int = 18,
) -> int:
    for text in bullets:
        draw.ellipse((x, y + 12, x + 18, y + 30), fill="#dbeafe")
        draw.ellipse((x + 5, y + 17, x + 13, y + 25), fill=dot_color)
        y = draw_wrapped_text(draw, (x + 48, y), text, font, INK, max_width - 48, line_gap=6)
        y += gap

    return y


def draw_mid_basis_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = draw_section_shell(
        draw,
        1235,
        780,
        "2｜预估收益率主要依据什么？",
    )

    inner_left, inner_right = left + 150, right - 150
    gap = 80
    box_w = int((inner_right - inner_left - gap * 3) / 4)
    box_h = 172
    box_y = content_top + 68

    items = [
        ("季度披露", "前十大持仓股", BLUE),
        ("相关指数 /", "ETF代理", GOLD),
        ("汇率等", "公开信息", BLUE),
        ("复合估算", "模型观察", GREEN),
    ]

    for idx, (line1, line2, color) in enumerate(items):
        x = inner_left + idx * (box_w + gap)

        rounded(draw, (x, box_y, x + box_w, box_y + box_h), 22, SOFT_CARD, LINE, 2)
        draw.rounded_rectangle((x, box_y, x + box_w, box_y + 18), radius=12, fill=color)

        draw_text_in_box(
            draw,
            (x + 20, box_y + 34, x + box_w - 20, box_y + box_h - 12),
            f"{line1}\n{line2}",
            font_size=60,
            min_font_size=44,
            bold=True,
            fill=INK,
            align="center",
            valign="center",
            line_gap=8,
        )

        if idx < len(items) - 1:
            draw_arrow(draw, x + box_w + 38, box_y + box_h // 2, x + box_w + gap - 30)

    note_top = box_y + box_h + 55
    note_box = (left + 110, note_top, right - 110, section_bottom - 55)
    rounded(draw, note_box, 28, "#fff8eb", "#e8cf9e", 2)

    title_text = "估算限制"
    limit_text = "部分基金衔接国内全球ETF，国内不开盘时无法形成新的可用估值，只能等待市场或公告更新。"

    text_left = note_box[0] + 170
    text_right = note_box[2] - 55
    text_width = text_right - text_left

    title_font = load_font(60, bold=True)
    body_font = load_font(54, bold=True)
    body_line_gap = 14
    title_body_gap = 24

    title_h = text_size(draw, title_text, title_font)[1]
    body_h = 0
    body_lines: list[str] = []
    body_boxes: list[tuple[int, int, int, int]] = []

    max_text_h = note_box[3] - note_box[1] - 74

    for size in range(54, 40, -2):
        candidate_font = load_font(size, bold=True)
        candidate_h, candidate_lines, candidate_boxes = measure_wrapped_text(
            draw,
            limit_text,
            candidate_font,
            text_width,
            body_line_gap,
        )

        total_h = title_h + title_body_gap + candidate_h
        body_font = candidate_font
        body_h = candidate_h
        body_lines = candidate_lines
        body_boxes = candidate_boxes

        if total_h <= max_text_h:
            break

    text_group_h = title_h + title_body_gap + body_h
    text_top = int(note_box[1] + (note_box[3] - note_box[1] - text_group_h) / 2)

    icon_size = 90
    icon_center_y = text_top + text_group_h / 2
    icon_y = int(icon_center_y - icon_size / 2)
    icon_x = note_box[0] + 48

    draw.ellipse((icon_x, icon_y, icon_x + icon_size, icon_y + icon_size), fill=GOLD)

    bang_font = load_font(76, bold=True)
    bang_bbox = draw.textbbox((0, 0), "!", font=bang_font)
    bang_w = bang_bbox[2] - bang_bbox[0]
    bang_h = bang_bbox[3] - bang_bbox[1]
    bang_x = icon_x + icon_size / 2 - bang_w / 2 - bang_bbox[0]
    bang_y = icon_y + icon_size / 2 - bang_h / 2 - bang_bbox[1]

    draw.text((bang_x, bang_y), "!", font=bang_font, fill="white")
    draw.text((text_left, text_top), title_text, font=title_font, fill=INK)

    body_y = text_top + title_h + title_body_gap
    for line, bbox in zip(body_lines, body_boxes):
        line_h = bbox[3] - bbox[1]
        draw.text((text_left - bbox[0], body_y - bbox[1]), line, font=body_font, fill=INK)
        body_y += line_h + body_line_gap


def draw_bottom_boundary_section(draw: ImageDraw.ImageDraw) -> None:
    left, content_top, right, section_bottom = draw_section_shell(
        draw,
        2100,
        850,
        "3｜模型边界与安全提醒",
    )

    card_y = content_top + 68
    card_gap = 70
    start_x = left + 110
    card_w = int((right - left - 220 - card_gap * 2) / 3)
    reminder_h = 145
    card_h = section_bottom - 55 - reminder_h - 45 - card_y

    cards = [
        (
            "模型优点",
            GREEN,
            ["公开数据可追溯", "便于个人学习复盘", "结果可与公告净值对照"],
        ),
        (
            "主要局限",
            GOLD,
            ["持仓披露存在滞后", "汇率、费用、现金仓位会影响结果", "估值时点和指数代理存在误差"],
        ),
        (
            "使用边界",
            RED,
            ["非实时净值，最终以公告为准", "不作为基金选择依据", "不提供任何投资建议"],
        ),
    ]

    for idx, (title, color, bullets) in enumerate(cards):
        x = start_x + idx * (card_w + card_gap)

        rounded(draw, (x, card_y, x + card_w, card_y + card_h), 28, SOFT_CARD, LINE, 2)
        rounded(draw, (x + 34, card_y + 38, x + card_w - 34, card_y + 116), 28, color, None)

        draw_text_in_box(
            draw,
            (x + 52, card_y + 40, x + card_w - 52, card_y + 112),
            title,
            font_size=58,
            min_font_size=44,
            bold=True,
            fill="white",
            align="center",
            valign="center",
        )

        draw_bullets_in_box(
            draw,
            (x + 64, card_y + 150, x + card_w - 45, card_y + card_h - 42),
            bullets,
            color,
            font_size=52,
            min_font_size=38,
            gap=14,
            valign="center",
        )

    reminder = (left + 110, card_y + card_h + 45, right - 110, section_bottom - 55)
    rounded(draw, reminder, 26, "#fffdf8", "#e7d0a3", 2)

    draw_text_in_box(
        draw,
        (reminder[0] + 45, reminder[1] + 25, reminder[2] - 45, reminder[3] - 25),
        "仅作为个人学习记录，不提供任何投资建议",
        font_size=76,
        min_font_size=54,
        bold=True,
        fill=RED,
        align="center",
        valign="center",
    )


def draw_footer(draw: ImageDraw.ImageDraw) -> None:
    footer_box = (150, 3000, WIDTH - 150, 3155)
    rounded(draw, footer_box, 28, "#fffdf8", "#e7d0a3", 2)

    draw_text_in_box(
        draw,
        (footer_box[0] + 45, footer_box[1] + 22, footer_box[2] - 45, footer_box[3] - 22),
        "个人模型预估｜仅供个人学习｜不构成任何投资建议",
        font_size=88,
        min_font_size=62,
        bold=True,
        fill=RED,
        align="center",
        valign="center",
    )

    signature = "鱼师AHNS · 个人公开数据建模复盘"
    sw, _ = text_size(draw, signature, FONT_SIGNATURE)
    draw.text((WIDTH - 150 - sw, 3168), signature, font=FONT_SIGNATURE, fill="#7b8796")


def build_image() -> Image.Image:
    image = Image.new("RGBA", (WIDTH, HEIGHT), BG)

    draw_watermarks(image)

    draw = ImageDraw.Draw(image)
    draw_title(draw)
    draw_top_time_section(draw)
    draw_mid_basis_section(draw)
    draw_bottom_boundary_section(draw)
    draw_footer(draw)

    return image.convert("RGB")


def main() -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    try:
        print("FONT_TITLE:", FONT_TITLE.getname())
        print("FONT_SUBTITLE:", FONT_SUBTITLE.getname())
    except Exception:
        pass

    image = build_image()
    image.save(OUTPUT_FILE, optimize=True, compress_level=9)

    print(f"说明图已生成: {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()