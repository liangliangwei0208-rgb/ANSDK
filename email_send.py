"""
qq_email_sender_simple.py

一个最简单、可外部调用的 QQ 邮箱发送模块。

功能：
1. 发送文字到 QQ 邮箱
2. 发送图片附件到 QQ 邮箱
3. 可选把图片嵌入邮件正文
4. 不需要 PowerShell 环境变量，直接在本文件顶部配置

重要：
    这个文件里包含 QQ 邮箱授权码，请不要上传到 GitHub、网盘、论文附件或公开代码仓库。
"""

from __future__ import annotations

import mimetypes
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from pathlib import Path
from typing import Sequence
import os

# =========================
# 直接在这里改配置即可
# =========================

# SENDER_EMAIL = "2569236501@qq.com"      # 发件 QQ 邮箱；如果授权码不是这个邮箱生成的，请改成对应发件邮箱
AUTH_CODE = "tnjljnhrupqheabe"          # QQ 邮箱 SMTP 授权码，不是 QQ 登录密码
DEFAULT_RECEIVER = "2569236501@qq.com"  # 默认收件人

SENDER_EMAIL = os.getenv("QQ_EMAIL_ACCOUNT")
AUTH_CODE = os.getenv("QQ_EMAIL_AUTH_CODE")

if not SENDER_EMAIL:
    raise ValueError("缺少环境变量 QQ_EMAIL_ACCOUNT")

if not AUTH_CODE:
    raise ValueError("缺少环境变量 QQ_EMAIL_AUTH_CODE")

SMTP_HOST = "smtp.qq.com"
SMTP_PORT = 465


def _to_list(value):
    """
    把单个邮箱/路径或列表统一转为列表。
    """
    if value is None:
        return []

    if isinstance(value, (str, Path)):
        return [value]

    return list(value)


def _escape_html(text: str) -> str:
    """
    HTML 转义，避免正文中的 < > & 破坏 HTML。
    """
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _guess_mime(file_path: Path) -> tuple[str, str]:
    """
    根据文件后缀推断 MIME 类型。
    """
    mime_type, _ = mimetypes.guess_type(str(file_path))

    if mime_type is None:
        return "application", "octet-stream"

    maintype, subtype = mime_type.split("/", 1)
    return maintype, subtype


def _check_files(image_paths: list[Path]) -> None:
    """
    检查图片文件是否存在。
    """
    for path in image_paths:
        if not path.exists():
            raise FileNotFoundError(f"图片文件不存在: {path}")

        if not path.is_file():
            raise FileNotFoundError(f"不是有效图片文件: {path}")


def build_message(
    subject: str,
    text: str,
    image_paths: str | Path | Sequence[str | Path] | None = None,
    to_email: str | Sequence[str] = DEFAULT_RECEIVER,
    sender_email: str = SENDER_EMAIL,
    embed_images: bool = True,
    attach_images: bool = True,
) -> EmailMessage:
    """
    构建邮件对象，不发送。

    参数：
        subject:
            邮件标题。
        text:
            邮件文字内容。
        image_paths:
            图片路径。可以是单个路径，也可以是路径列表。
        to_email:
            收件人。可以是一个邮箱，也可以是多个邮箱列表。
        sender_email:
            发件邮箱。
        embed_images:
            是否把图片嵌入正文。
        attach_images:
            是否把图片作为附件发送。
    """
    receivers = [str(x) for x in _to_list(to_email)]
    images = [Path(x) for x in _to_list(image_paths)]
    _check_files(images)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = ", ".join(receivers)
    msg["Date"] = formatdate(localtime=True)

    # 纯文本正文
    msg.set_content(text or "")

    # HTML 正文，可嵌入图片
    if embed_images and images:
        html_parts = [
            "<html><body>",
            "<pre style='font-family: Microsoft YaHei, SimHei, Arial, sans-serif; "
            "font-size: 14px; white-space: pre-wrap;'>",
            _escape_html(text or ""),
            "</pre>",
        ]

        related_images = []

        for image_path in images:
            maintype, subtype = _guess_mime(image_path)
            if maintype != "image":
                continue

            cid = make_msgid(domain="qq_email_sender")
            related_images.append((image_path, cid, maintype, subtype))

            html_parts.append(
                f"<p><img src='cid:{cid[1:-1]}' "
                f"style='max-width: 100%; height: auto;'></p>"
            )

        html_parts.append("</body></html>")
        msg.add_alternative("\n".join(html_parts), subtype="html")

        html_part = msg.get_payload()[-1]

        for image_path, cid, maintype, subtype in related_images:
            with image_path.open("rb") as f:
                html_part.add_related(
                    f.read(),
                    maintype=maintype,
                    subtype=subtype,
                    cid=cid,
                    filename=image_path.name,
                )

    # 图片附件
    if attach_images and images:
        for image_path in images:
            maintype, subtype = _guess_mime(image_path)

            with image_path.open("rb") as f:
                msg.add_attachment(
                    f.read(),
                    maintype=maintype,
                    subtype=subtype,
                    filename=image_path.name,
                )

    return msg


def send_email(
    subject: str,
    text: str,
    image_paths: str | Path | Sequence[str | Path] | None = None,
    to_email: str | Sequence[str] = DEFAULT_RECEIVER,
    sender_email: str = SENDER_EMAIL,
    auth_code: str = AUTH_CODE,
    embed_images: bool = True,
    attach_images: bool = True,
    timeout: int = 30,
) -> bool:
    """
    发送邮件。

    最简单调用：
        send_email(
            subject="RSI 分析结果",
            text="图片已生成，请查收。",
            image_paths=["output/nasdaq_analysis.png", "output/honglidibo.png"],
        )

    只发文字：
        send_email(
            subject="测试邮件",
            text="这是一封测试邮件。",
        )

    返回：
        成功返回 True；失败会抛出异常。
    """
    msg = build_message(
        subject=subject,
        text=text,
        image_paths=image_paths,
        to_email=to_email,
        sender_email=sender_email,
        embed_images=embed_images,
        attach_images=attach_images,
    )

    receivers = [str(x) for x in _to_list(to_email)]
    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=timeout, context=context) as smtp:
        smtp.login(sender_email, auth_code)
        smtp.send_message(msg, from_addr=sender_email, to_addrs=receivers)

    return True


def send_text(subject: str, text: str, to_email: str | Sequence[str] = DEFAULT_RECEIVER) -> bool:
    """
    只发送文字。
    """
    return send_email(
        subject=subject,
        text=text,
        image_paths=None,
        to_email=to_email,
        embed_images=False,
        attach_images=False,
    )


def send_images(
    subject: str,
    text: str,
    image_paths: str | Path | Sequence[str | Path],
    to_email: str | Sequence[str] = DEFAULT_RECEIVER,
) -> bool:
    """
    发送文字 + 图片。
    """
    return send_email(
        subject=subject,
        text=text,
        image_paths=image_paths,
        to_email=to_email,
        embed_images=True,
        attach_images=True,
    )


if __name__ == "__main__":
    # 不建议直接运行本模块。
    # 推荐在 main.py 中外部调用 send_email()。
    pass
