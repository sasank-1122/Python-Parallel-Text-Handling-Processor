# app/search_export/emailer2.py
from email.message import EmailMessage
import os
import json
import logging
from typing import List, Dict, Any, Optional
from .search_save import save_to_csv

logger = logging.getLogger(__name__)

def build_summary_email(check_results: List[Dict[str, Any]], smtp_from: str, smtp_to: str, min_score_alert: Optional[float] = None) -> EmailMessage:
    total = len(check_results)
    avg_score = sum((r.get("score") or 0) for r in check_results) / total if total else 0.0
    sorted_results = sorted(check_results, key=lambda r: r.get("score", 0), reverse=True)
    top = sorted_results[:5]
    low = sorted_results[-5:]
    body_lines = [
        f"Summary of checks",
        f"Total checked: {total}",
        f"Average score: {avg_score:.2f}",
        "",
        "Top items (highest scores):"
    ]
    for r in top:
        body_lines.append(f"- UID: {r.get('uid')} | score: {r.get('score')} | details: {json.dumps(r.get('details'), ensure_ascii=False)}")

    if min_score_alert is not None:
        alerts = [r for r in check_results if (r.get("score") or 0) <= min_score_alert]
        if alerts:
            body_lines.append("")
            body_lines.append(f"ALERTS: {len(alerts)} items with score <= {min_score_alert}")
            for a in alerts[:10]:
                body_lines.append(f"!! UID: {a.get('uid')} | score={a.get('score')} | details={json.dumps(a.get('details'), ensure_ascii=False)}")
    text_body = "\n".join(body_lines)
    html_body = f"<html><body><pre>{text_body}</pre></body></html>"
    msg = EmailMessage()
    msg["Subject"] = f"Checks Summary - {total} items"
    msg["From"] = smtp_from
    msg["To"] = smtp_to
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")
    logger.info("Built summary email for %d items", total)
    return msg

def attach_file(msg: EmailMessage, file_path: str, mime_type: str = "text/csv"):
    if not os.path.exists(file_path):
        logger.warning("Attachment not found: %s", file_path)
        return
    with open(file_path, "rb") as fh:
        data = fh.read()
    maintype, subtype = mime_type.split("/", 1)
    msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=os.path.basename(file_path))
    logger.info("Attached file %s", file_path)

def send_email(msg: EmailMessage, host: str, port: int, user: str, password: str, use_starttls: bool = True):
    import smtplib
    try:
        with smtplib.SMTP(host, port, timeout=30) as s:
            s.ehlo()
            if use_starttls:
                s.starttls()
                s.ehlo()
            if user and password:
                s.login(user, password)
            s.send_message(msg)
            logger.info("Email sent to %s", msg["To"])
    except Exception:
        logger.exception("Failed to send email")
        raise
