"""
commentary.py — Human-readable trade commentary generator.

Every trade that passes through the system gets two commentary blocks:

  entry_commentary: "What triggered this trade, what the indicators showed,
                      why the risk engine approved it."

  forward_strategy: "What happens from here — TP target, SL level,
                      exit conditions, any add-on rules, time limits."

These are stored in the position/trade metadata and displayed on the dashboard.
They make dry-run review meaningful: you can see exactly what the bot was thinking.
"""

from typing import Dict, Any, Optional
from datetime import datetime, timezone


def generate_entry_commentary(
    bot_id:     str,
    inst_id:    str,
    direction:  str,
    entry_price: float,
    sl_price:   float,
    tp_price:   Optional[float],
    kelly_frac: float,
    size_usdt:  float,
    metadata:   Dict[str, Any],
) -> Dict[str, str]:
    """
    Returns {"trigger": "...", "forward_strategy": "...", "summary": "..."}
    """
    trigger          = _build_trigger(bot_id, direction, inst_id, entry_price, metadata)
    forward_strategy = _build_forward(bot_id, direction, entry_price,
                                       sl_price, tp_price, size_usdt, kelly_frac, metadata)
    risk_pct = abs(entry_price - sl_price) / entry_price * 100

    summary = (
        f"{direction} {inst_id} @ {entry_price:.4f} | "
        f"Risk: {risk_pct:.2f}% | "
        f"Size: ${size_usdt:.0f} | "
        f"Kelly: {kelly_frac*100:.1f}%"
    )

    return {
        "trigger":          trigger,
        "forward_strategy": forward_strategy,
        "summary":          summary,
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "bot_id":           bot_id,
    }


def generate_exit_commentary(
    bot_id:       str,
    inst_id:      str,
    side:         str,
    entry_price:  float,
    exit_price:   float,
    size:         float,
    net_pnl:      float,
    close_reason: str,
    duration_s:   int,
    metadata:     Dict[str, Any],
) -> str:
    """Returns a plain-language exit description."""
    direction  = "Long" if side == "LONG" else "Short"
    pnl_emoji  = "✅" if net_pnl >= 0 else "❌"
    pnl_pct    = abs(exit_price - entry_price) / entry_price * 100
    won        = net_pnl >= 0
    duration   = _format_duration(duration_s)

    reason_map = {
        "paper_TP":                  f"{pnl_emoji} Take-profit hit at {exit_price:.4f} (+{pnl_pct:.2f}%). Trade worked as planned.",
        "paper_SL":                  f"{pnl_emoji} Stop-loss triggered at {exit_price:.4f} (-{pnl_pct:.2f}%). Setup invalidated — price moved against the thesis.",
        "time_exit":                 f"⏱️ Time exit after {duration}. Price hadn't reached TP or SL — flat exit to free capital.",
        "mean_reversion_tp":         f"{pnl_emoji} Mean-reversion target reached. Price reverted {pnl_pct:.2f}% toward the 20-SMA.",
        "first_target_long":         f"📈 First target hit (+{pnl_pct:.2f}%). 50% of position closed, trailing stop now protecting the rest.",
        "first_target_short":        f"📉 First target hit (+{pnl_pct:.2f}%). 50% of position closed, trailing stop now protecting the rest.",
        "funding_rate_normalised":   f"💸 Funding rate fell below exit threshold. Collected funding and exited cleanly.",
        "funding_rate_declining":    f"💸 Funding rate declining for 2+ consecutive periods — exit before edge disappears.",
        "loss_exceeds_collected_funding": f"⚠️ Unrealized loss exceeded 2× collected funding. Emergency exit to protect capital.",
        "rebalance":                 f"🔄 Rebalance: asset no longer in top/bottom momentum tier. Position rotated.",
        "reconcile_not_found_on_exchange": f"⚠️ Position not found on exchange during startup reconcile — marked closed.",
    }

    explanation = reason_map.get(
        close_reason,
        f"Closed: {close_reason.replace('_', ' ')}. P&L: {net_pnl:+.2f} USDT ({'+' if won else ''}{pnl_pct:.2f}%)."
    )

    return (
        f"{explanation}\n"
        f"Held for {duration}. "
        f"Entry {entry_price:.4f} → Exit {exit_price:.4f}. "
        f"Net P&L: {net_pnl:+.2f} USDT."
    )


# ── Per-bot trigger builders ──────────────────────────────────────────────────

def _build_trigger(bot_id: str, direction: str, inst_id: str,
                    entry_price: float, meta: Dict) -> str:
    m = meta

    if bot_id == "bot1_scalper":
        rsi  = m.get("rsi",  "—")
        vol  = m.get("vol_ratio", "—")
        side = "crossed ABOVE 55" if direction == "LONG" else "crossed BELOW 45"
        ema_rel = "above" if direction == "LONG" else "below"
        return (
            f"5-minute RSI {side} (current: {rsi:.1f}) "
            f"with volume {vol:.1f}× the 20-bar average. "
            f"Price is {ema_rel} the 20 EMA — momentum confirmed."
        )

    elif bot_id == "bot2_trend":
        adx = m.get("adx", "—")
        rsi = m.get("rsi", "—")
        cross = "Golden cross (50 EMA crossed above 200 EMA)" if direction == "LONG" \
                else "Death cross (50 EMA crossed below 200 EMA)"
        slope = "upward" if direction == "LONG" else "downward"
        return (
            f"{cross} confirmed within last 5 bars on 4H chart. "
            f"1D 200 EMA slope is {slope}. "
            f"ADX: {adx:.1f} (trend strength confirmed above 25). "
            f"RSI: {rsi:.1f} (within 45–65 neutral zone — not overbought/oversold at entry)."
        )

    elif bot_id == "bot3_meanrev":
        adx = m.get("adx", "—")
        rsi = m.get("rsi", "—")
        sma = m.get("sma", entry_price)
        bb_side = "below the lower Bollinger Band" if direction == "LONG" else "above the upper Bollinger Band"
        rsi_cond = f"oversold (RSI {rsi:.1f} < 28)" if direction == "LONG" else f"overbought (RSI {rsi:.1f} > 72)"
        return (
            f"Market is ranging: ADX {adx:.1f} < 20 (no trend). "
            f"Price closed {bb_side} on the prior bar, confirming extreme — "
            f"then closed back inside on this bar (1-bar confirmation). "
            f"RSI is {rsi_cond}. "
            f"20-SMA target: {float(sma):.4f}."
        )

    elif bot_id == "bot5_breakout":
        box_high  = m.get("box_high", 0)
        box_low   = m.get("box_low", 0)
        box_h     = m.get("box_height", 0)
        is_retest = m.get("retest", False)
        retest_note = " Price then pulled back to test the broken level and held — retest confirmed, higher-confidence entry." if is_retest else " No retest occurred — entry at breakout close."
        side_desc = "resistance" if direction == "LONG" else "support"
        return (
            f"Consolidation box detected: {box_low:.4f} – {box_high:.4f} "
            f"(range: {box_h:.4f}, within 2.5% threshold). "
            f"This bar closed outside {side_desc} on volume > 2× the 20-bar average — "
            f"breakout confirmed.{retest_note}"
        )

    elif bot_id == "bot6_funding":
        rate      = m.get("entry_funding_rate", 0)
        ann_rate  = rate * 3 * 365 * 100
        payer     = "longs paying shorts" if direction == "SHORT" else "shorts paying longs"
        return (
            f"8-hour funding rate: {rate*100:.4f}% ({payer}). "
            f"Annualised yield: ~{ann_rate:.1f}%. "
            f"Spot-perp basis checked and within 0.20% threshold. "
            f"Entering {direction.lower()} perp to collect funding payments."
        )

    elif bot_id == "bot7_momentum":
        target_pct = m.get("target_pct", 0)
        strat      = m.get("strategy", "")
        tier       = "top-3 momentum" if "long" in strat else "bottom-2 momentum"
        return (
            f"Daily rebalance: {inst_id} ranked in {tier} tier. "
            f"Momentum score = (Close − Close[20]) / ATR(20). "
            f"Correlation with other held assets < 0.85 — included. "
            f"Target weight: {target_pct:.0f}% of account."
        )

    else:
        return f"Signal generated by {bot_id} for {direction} on {inst_id} at {entry_price:.4f}."


def _build_forward(bot_id: str, direction: str, entry_price: float,
                    sl_price: float, tp_price: Optional[float],
                    size_usdt: float, kelly_frac: float,
                    meta: Dict) -> str:
    risk_pct  = abs(entry_price - sl_price) / entry_price * 100
    parts = []

    # TP
    if tp_price:
        rr   = abs(tp_price - entry_price) / abs(entry_price - sl_price) if sl_price != entry_price else 0
        tp_d = "above" if direction == "LONG" else "below"
        parts.append(
            f"Take-profit: {tp_price:.4f} ({tp_d} entry by "
            f"{abs(tp_price-entry_price)/entry_price*100:.2f}%) — R:R ≈ {rr:.1f}:1."
        )

    # SL
    sl_d = "below" if direction == "LONG" else "above"
    parts.append(f"Stop-loss: {sl_price:.4f} ({sl_d} entry, {risk_pct:.2f}% risk).")

    # Bot-specific rules
    if bot_id == "bot1_scalper":
        parts.append("Time exit: auto-close after 8 minutes if TP/SL not hit. Cooldown: 3 min before next trade on this asset.")

    elif bot_id == "bot2_trend":
        parts.append("Partial exit: 50% of position will close at +4% — locking profit. Remaining half rides with trailing stop (2× ATR). Stop moves up as price moves in favour.")
        kelly_adj = meta.get("kelly_adj", 1.0)
        if kelly_adj < 1.0:
            parts.append(f"Note: ADX was 25–40, so Kelly reduced to 60%. Full Kelly kicks in above ADX 40.")

    elif bot_id == "bot3_meanrev":
        sma = meta.get("sma", 0)
        parts.append(f"Target: 50% reversion to 20-SMA ({float(sma):.4f}). Position closes when price reaches the halfway point between entry and the SMA. No trailing stop — clean exit at mean.")

    elif bot_id == "bot5_breakout":
        box_h = meta.get("box_height", 0)
        tp_level = entry_price + box_h * 1.5 if direction == "LONG" else entry_price - box_h * 1.5
        is_retest = meta.get("is_retest_add", False)
        parts.append(f"TP = 1.5× box height ({float(box_h):.4f}) from breakout = {tp_level:.4f}.")
        parts.append(f"SL = back inside the box — price invalidates the breakout if it closes back inside.")
        if is_retest:
            parts.append("This is the retest add-on (50% extra). Combined with the initial breakout entry for full position.")

    elif bot_id == "bot6_funding":
        parts.append(f"Minimum hold: 3 funding payments (24 hours). ")
        parts.append(f"Exit conditions: rate falls below 0.01% OR rate declines for 2 consecutive 8h periods.")
        parts.append(f"Emergency exit: if unrealized loss exceeds 2× collected funding at any time.")

    elif bot_id == "bot7_momentum":
        parts.append("Next rebalance: daily at 00:00 UTC. Position held until asset falls out of momentum tier OR portfolio composition changes > 10%.")
        parts.append("Weekly guard: if portfolio drops 5% in 7 days, bot halts for 72 hours.")

    parts.append(f"Position size: ${size_usdt:.0f} USDT ({kelly_frac*100:.1f}% half-Kelly).")

    return " ".join(parts)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _format_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    elif seconds < 86400:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h {m}m"
    else:
        d = seconds // 86400
        h = (seconds % 86400) // 3600
        return f"{d}d {h}h"
