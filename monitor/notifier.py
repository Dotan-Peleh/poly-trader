"""
Telegram notifier — same pattern as crypto-trader, namespaced with
🎲 [POLY] so messages from both bots can co-exist in one chat.

Stdlib only (urllib) so we don't fight library/SSL conflicts on the VM.
"""
import json
import logging
import urllib.parse
import urllib.request

from config.settings import settings

logger = logging.getLogger(__name__)


class Notifier:
    def __init__(self):
        self._token = settings.telegram_bot_token or ""
        self._chat = settings.telegram_chat_id or ""
        self._prefix = settings.telegram_message_prefix or ""
        if self._token and self._chat:
            logger.info("Notifier: Telegram enabled")
        else:
            logger.warning("Notifier: Telegram NOT configured (missing token or chat)")

    def send(self, message: str):
        text = f"{self._prefix} {message}".strip() if self._prefix else message
        if not (self._token and self._chat):
            logger.info(f"[NOTIFY] {text}")
            return
        try:
            url = f"https://api.telegram.org/bot{self._token}/sendMessage"
            data = urllib.parse.urlencode({
                "chat_id": self._chat,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            }).encode("utf-8")
            req = urllib.request.Request(url, data=data, method="POST")
            with urllib.request.urlopen(req, timeout=8) as r:
                ok = json.loads(r.read().decode("utf-8")).get("ok", False)
                if not ok:
                    logger.error("Telegram returned ok=False")
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
