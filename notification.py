"""Email notification system for trade opportunities.

Sends rich HTML trade alerts via Gmail SMTP (App Password) with
approve/deny links. Requires a Gmail account with 2FA and an
App Password configured.

Usage:
    notifier = TradeNotifier(
        smtp_user="you@gmail.com",
        smtp_password="abcd efgh ijkl mnop",  # App Password
        from_addr="you@gmail.com",
        to_addr="you@gmail.com",
        approval_base_url="http://localhost:8080",
    )
    notifier.send_trade_alert(opp_details, state_summary, backtest_data, token)
"""

import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, Optional

logger = logging.getLogger("notification")


class TradeNotifier:
    """Sends trade opportunity alerts via email with approve/deny workflow."""

    def __init__(
        self,
        smtp_host: str = "smtp.gmail.com",
        smtp_port: int = 465,
        smtp_user: str = "",
        smtp_password: str = "",
        from_addr: str = "",
        to_addr: str = "",
        approval_base_url: str = "http://localhost:8080",
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.from_addr = from_addr or smtp_user
        self.to_addr = to_addr or smtp_user
        self.approval_base_url = approval_base_url.rstrip("/")

    def send_trade_alert(
        self,
        opp: Dict[str, Any],
        state: Dict[str, Any],
        verdict: Optional[Dict[str, Any]] = None,
        token: str = "",
    ) -> bool:
        """Send a trade opportunity email with approve/deny links.

        Returns True if the email was sent successfully.
        """
        subject = self._build_subject(opp)
        html = self._build_html(opp, state, verdict, token)
        return self._send(subject, html)

    def _build_subject(self, opp: Dict[str, Any]) -> str:
        side_icon = "🟢" if opp.get("side") == "BUY" else "🔴"
        return (
            f"[TRADE ALERT] {side_icon} {opp.get('type', '').upper()} "
            f"{opp.get('side', '')} ${opp.get('size_usd', 0):.0f} "
            f"{opp.get('currency', '')}"
        )

    def _build_html(
        self,
        opp: Dict[str, Any],
        state: Dict[str, Any],
        verdict: Optional[Dict[str, Any]] = None,
        token: str = "",
    ) -> str:
        approve_url = f"{self.approval_base_url}/approve/{token}"
        deny_url = f"{self.approval_base_url}/deny/{token}"

        risk_reward_rows = ""
        if verdict:
            rr = self._compute_risk_reward(opp, verdict)
            risk_reward_rows = f"""
            <tr><td colspan="2" style="padding:8px 12px;background:#f8f9fa;font-weight:bold;border-bottom:1px solid #dee2e6;">Risk / Reward</td></tr>
            <tr><td style="padding:6px 12px;color:#666;">Win Rate</td><td style="padding:6px 12px;text-align:right;">{verdict.get('win_rate', 0)*100:.0f}%</td></tr>
            <tr><td style="padding:6px 12px;color:#666;">Sharpe Ratio</td><td style="padding:6px 12px;text-align:right;">{verdict.get('sharpe_ratio', 0):.2f}</td></tr>
            <tr><td style="padding:6px 12px;color:#666;">Profit Factor</td><td style="padding:6px 12px;text-align:right;">{verdict.get('profit_factor', 0):.2f}</td></tr>
            <tr><td style="padding:6px 12px;color:#666;">Max Drawdown</td><td style="padding:6px 12px;text-align:right;">{verdict.get('max_drawdown_pct', 0):.1f}%</td></tr>
            <tr><td style="padding:6px 12px;color:#666;">Fee Impact</td><td style="padding:6px 12px;text-align:right;">{rr['fee_pct']:.2f}%</td></tr>
            <tr><td style="padding:6px 12px;color:#666;">Expected Value<br><small style="color:#999;">({verdict.get('strategy', '?')} on {verdict.get('currency', '?')})</small></td>
                <td style="padding:6px 12px;text-align:right;font-weight:bold;{'color:#28a745' if rr['ev'] > 0 else 'color:#dc3545'};">
                    {f'+${rr["ev"]:.0f}' if rr['ev'] > 0 else f'-${abs(rr["ev"]):.0f}'}</td></tr>
            """

        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;margin:0;padding:0;background:#f4f4f4;">
<table cellpadding="0" cellspacing="0" style="width:100%;max-width:600px;margin:20px auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1);">
<thead>
<tr><td style="padding:16px 20px;background:#1a1a2e;color:#fff;font-size:18px;font-weight:600;">
    {'🟢 BUY' if opp.get('side') == 'BUY' else '🔴 SELL'} &nbsp;
    {opp.get('currency', '')} &nbsp; ${opp.get('size_usd', 0):.0f}
</td></tr>
</thead>
<tbody>
<tr><td style="padding:0 20px;">
<table cellpadding="0" cellspacing="0" style="width:100%;">
    <tr><td style="padding:8px 0;border-bottom:1px solid #eee;color:#666;">Type</td>
        <td style="padding:8px 0;border-bottom:1px solid #eee;text-align:right;font-weight:500;">{opp.get('type', '').upper()}</td></tr>
    <tr><td style="padding:8px 0;border-bottom:1px solid #eee;color:#666;">Size</td>
        <td style="padding:8px 0;border-bottom:1px solid #eee;text-align:right;font-weight:500;">${opp.get('size_usd', 0):.2f}</td></tr>
    <tr><td style="padding:8px 0;border-bottom:1px solid #eee;color:#666;">Expected Fee</td>
        <td style="padding:8px 0;border-bottom:1px solid #eee;text-align:right;font-weight:500;">${opp.get('expected_fee', 0):.2f}</td></tr>
    <tr><td style="padding:8px 0;border-bottom:1px solid #eee;color:#666;">Priority</td>
        <td style="padding:8px 0;border-bottom:1px solid #eee;text-align:right;font-weight:500;">{opp.get('priority', 0):.2f}</td></tr>
    <tr><td colspan="2" style="padding:12px 0;color:#555;font-style:italic;">{opp.get('reason', '')}</td></tr>
    {risk_reward_rows}
</table>
</td></tr>
<tr><td style="padding:20px;text-align:center;border-top:1px solid #eee;">
    <a href="{approve_url}" style="display:inline-block;padding:12px 32px;margin:0 8px 8px 0;background:#28a745;color:#fff;text-decoration:none;border-radius:6px;font-weight:600;font-size:15px;">✅ Approve</a>
    <a href="{deny_url}" style="display:inline-block;padding:12px 32px;margin:0 0 8px 0;background:#dc3545;color:#fff;text-decoration:none;border-radius:6px;font-weight:600;font-size:15px;">❌ Deny</a>
</td></tr>
<tr><td style="padding:12px 20px;background:#f8f9fa;color:#999;font-size:12px;text-align:center;">
    Token: {token[:12]}… | Portfolio: ${state.get('total_value', 0):.0f} | USDC: ${state.get('usdc_balance', 0):.0f}
</td></tr>
</tbody>
</table>
</body>
</html>"""

    def _compute_risk_reward(
        self, opp: Dict[str, Any], verdict: Dict[str, Any]
    ) -> Dict[str, Any]:
        win_rate = verdict.get("win_rate", 0)
        profit_factor = verdict.get("profit_factor", 1.0)
        max_dd = verdict.get("max_drawdown_pct", 0)
        size = opp.get("size_usd", 0)
        fee = opp.get("expected_fee", 0)
        fee_pct = (fee / size * 100) if size > 0 else 0

        if win_rate > 0 and profit_factor > 0 and profit_factor != 1.0:
            avg_win_loss_ratio = profit_factor * (1 - win_rate) / max(win_rate, 0.001)
            ev = win_rate * avg_win_loss_ratio - (1 - win_rate)
            ev_dollars = ev * size
        else:
            ev_dollars = 0

        return {
            "ev": ev_dollars,
            "fee_pct": fee_pct,
            "max_dd": max_dd,
        }

    def _send(self, subject: str, html: str) -> bool:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.from_addr
        msg["To"] = self.to_addr
        msg.attach(MIMEText(html, "html"))

        try:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(
                self.smtp_host, self.smtp_port, context=context
            ) as server:
                if self.smtp_user and self.smtp_password:
                    server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.from_addr, [self.to_addr], msg.as_string())
            logger.info("Email sent: %s", subject)
            return True
        except Exception as e:
            logger.error("Failed to send email: %s", e)
            return False
