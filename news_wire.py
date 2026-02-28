"""
news_wire.py  —  Phase 22: Unified AI Hub (OpenRouter)

Changes vs Phase 6/prior:
  [P22-1] Removes ANTHROPIC_API_KEY dependency.  VADER scoring is preserved
          as the primary local engine.  An optional OpenRouter LLM enrichment
          pass can be enabled via OPENROUTER_API_KEY to re-score or summarise
          the headline batch before VADER runs, improving accuracy without
          requiring any Anthropic credentials.

          If OPENROUTER_API_KEY is absent or the call fails, the system
          silently falls back to pure VADER — no exception is raised and
          no field is left null.  The `source` field in MacroBias now
          distinguishes "vader_only", "openrouter+vader", or
          "openrouter_failed+vader" so the dashboard can render the
          correct status.

Headline source : CoinTelegraph public RSS feed (no auth required)
Scoring engine  : vaderSentiment (local) + optional OpenRouter enrichment
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import aiohttp

log = logging.getLogger("news_wire")

# ── Config ─────────────────────────────────────────────────────────────────────
NEWS_CACHE_TTL         = float(os.environ.get("NEWS_CACHE_TTL",               "300.0"))
NEWS_REFRESH_INTERVAL  = float(os.environ.get("NEWS_REFRESH_INTERVAL",        "240.0"))
NEWS_BEARISH_THRESHOLD = float(os.environ.get("NEWS_BEARISH_BLOCK_THRESHOLD", "-0.5"))
NEWS_MAX_HEADLINES     = int  (os.environ.get("NEWS_MAX_HEADLINES",           "30"))
NEWS_FETCH_TIMEOUT     = float(os.environ.get("NEWS_FETCH_TIMEOUT",           "15.0"))

# [P22-1] OpenRouter optional enrichment
OPENROUTER_API_KEY   = os.environ.get("OPENROUTER_API_KEY",   "")
OPENROUTER_MODEL     = os.environ.get("OPENROUTER_MODEL",     "google/gemini-2.0-flash-001:free")
OPENROUTER_SITE_URL  = os.environ.get("OPENROUTER_SITE_URL",  "https://github.com/powertrader")
OPENROUTER_SITE_NAME = os.environ.get("OPENROUTER_SITE_NAME", "PowerTrader AI")
# Keep the NewsWire LLM call well inside the P17 hard cap
_NW_OR_TIMEOUT = float(os.environ.get("NEWS_OPENROUTER_TIMEOUT", "30.0"))
_OPENROUTER_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"

_NEWS_URL = "https://cointelegraph.com/rss"

# ── Lazy-loaded VADER ──────────────────────────────────────────────────────────
_vader_analyser = None


def _get_vader():
    global _vader_analyser
    if _vader_analyser is not None:
        return _vader_analyser
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer  # type: ignore
        _vader_analyser = SentimentIntensityAnalyzer()
        log.info("NewsWire: VADER SentimentIntensityAnalyzer loaded.")
        return _vader_analyser
    except ImportError:
        log.error(
            "NewsWire: vaderSentiment not installed. "
            "Run: pip install vaderSentiment"
        )
        return None


# ══════════════════════════════════════════════════════════════════════════════
# MacroBias dataclass
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class MacroBias:
    score:     float
    label:     str
    headlines: List[str] = field(default_factory=list)
    source:    str       = "unknown"
    ts:        float     = field(default_factory=time.time)
    # [P22-1] enrichment metadata — never None, always has a defined state
    llm_enriched:  bool           = False
    llm_error:     Optional[str]  = None
    llm_latency_ms: float         = 0.0

    @staticmethod
    def neutral(reason: str = "fallback") -> "MacroBias":
        return MacroBias(
            score=0.0, label="Neutral", source=reason,
            llm_enriched=False, llm_error=reason, llm_latency_ms=0.0,
        )

    @staticmethod
    def _label_from_score(score: float) -> str:
        if score >=  0.7: return "Extreme Greed / Strongly Bullish"
        if score >=  0.3: return "Bullish"
        if score >=  0.1: return "Mildly Bullish"
        if score >  -0.1: return "Neutral"
        if score >  -0.3: return "Mildly Bearish"
        if score >  -0.7: return "Bearish"
        return "Extreme Fear / Strongly Bearish"


# ══════════════════════════════════════════════════════════════════════════════
# [P22-1] OpenRouter headline enrichment (NewsWire flavour)
# ══════════════════════════════════════════════════════════════════════════════

async def _openrouter_macro_score(
    headlines: List[str],
    session: aiohttp.ClientSession,
) -> Tuple[Optional[float], float, Optional[str]]:
    """
    Asks OpenRouter to rate the aggregate macro sentiment of a headline batch.

    Returns
    -------
    (score_override, latency_ms, error)
        score_override : float ∈ [-1, 1] mapped to MacroBias scale, or None on error.
        latency_ms     : wall-clock time of the request.
        error          : structured error string or None.

    The returned score is on the same [-1, +1] VADER scale so it can be
    used directly as MacroBias.score or blended with the VADER result.
    """
    if not OPENROUTER_API_KEY:
        return None, 0.0, "no_api_key"

    snippet = "\n".join(f"- {h}" for h in headlines[:15])
    prompt  = (
        "You are a macro sentiment analyst for cryptocurrency markets.\n"
        "Rate the aggregate sentiment of the following headlines on a scale "
        "from -1.00 (extremely bearish) to +1.00 (extremely bullish).\n"
        f"Headlines:\n{snippet}\n\n"
        'Respond with ONLY valid JSON: {"score": <float -1 to 1>, "summary": "<10 words max>"}'
    )

    payload = {
        "model":       OPENROUTER_MODEL,
        "messages":    [{"role": "user", "content": prompt}],
        "max_tokens":  60,
        "temperature": 0.1,
    }
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "HTTP-Referer":  OPENROUTER_SITE_URL,
        "X-Title":       OPENROUTER_SITE_NAME,
        "Content-Type":  "application/json",
    }

    t0 = time.monotonic()
    try:
        async with session.post(
            _OPENROUTER_ENDPOINT,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=_NW_OR_TIMEOUT),
        ) as resp:
            latency_ms = (time.monotonic() - t0) * 1_000
            if resp.status != 200:
                body = await resp.text()
                err  = f"http_{resp.status}: {body[:120]}"
                log.warning("[P22-1/NewsWire] OpenRouter status %s: %s", resp.status, err)
                return None, latency_ms, err

            data = await resp.json(content_type=None)

        content = (
            data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
        )
        content = content.replace("```json", "").replace("```", "").strip()
        parsed  = json.loads(content)
        score   = float(parsed["score"])
        score   = max(-1.0, min(1.0, score))
        log.info(
            "[P22-1/NewsWire] OpenRouter macro: score=%.4f lat=%.1fms summary=%s",
            score, latency_ms, parsed.get("summary", ""),
        )
        return score, latency_ms, None

    except asyncio.TimeoutError:
        lat = (time.monotonic() - t0) * 1_000
        log.warning("[P22-1/NewsWire] OpenRouter timeout after %.1fms", lat)
        return None, lat, "timeout"
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        lat = (time.monotonic() - t0) * 1_000
        log.warning("[P22-1/NewsWire] OpenRouter parse error: %s", exc)
        return None, lat, f"parse_error:{exc}"
    except Exception as exc:
        lat = (time.monotonic() - t0) * 1_000
        log.warning("[P22-1/NewsWire] OpenRouter unexpected error: %s", exc)
        return None, lat, f"error:{exc}"


# ══════════════════════════════════════════════════════════════════════════════
# NewsWire
# ══════════════════════════════════════════════════════════════════════════════

class NewsWire:
    """
    Macro sentiment feed with optional OpenRouter LLM enrichment.

    Scoring pipeline:
      1. Fetch CoinTelegraph RSS headlines.
      2. Score locally with VADER → `vader_score` ∈ [-1, +1].
      3. If OPENROUTER_API_KEY present, call OpenRouter for an independent
         macro score → `llm_score`.
      4. Final score = blend: 60 % VADER + 40 % LLM (if LLM available),
         otherwise 100 % VADER.
      5. Populate MacroBias with source tag indicating which engines ran.

    All fields in MacroBias are always populated with defined values —
    never left as None — so the dashboard can render without null-guards.
    """

    def __init__(self):
        self._bias:    Optional[MacroBias]          = None
        self._lock     = asyncio.Lock()
        self._running  = False
        self._session: Optional[aiohttp.ClientSession] = None
        if OPENROUTER_API_KEY:
            log.info(
                "[P22-1/NewsWire] OpenRouter enrichment ENABLED — model=%s",
                OPENROUTER_MODEL,
            )
        else:
            log.info(
                "[P22-1/NewsWire] OPENROUTER_API_KEY not set — "
                "using VADER-only scoring."
            )

    # ── Lifecycle ──────────────────────────────────────────────────────────────
    async def start(self):
        timeout       = aiohttp.ClientTimeout(total=NEWS_FETCH_TIMEOUT)
        self._session = aiohttp.ClientSession(timeout=timeout)
        self._running = True
        await self._refresh()

    async def close(self):
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()

    # ── RSS fetch ──────────────────────────────────────────────────────────────
    async def _fetch_rss(self) -> List[str]:
        user_agents = [
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
        ]
        headers = {
            "User-Agent":    random.choice(user_agents),
            "Accept":        "application/rss+xml, text/xml, */*",
            "Cache-Control": "no-cache",
        }
        try:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            async with self._session.get(_NEWS_URL, headers=headers) as r:
                if r.status != 200:
                    log.debug("[NewsWire] RSS status %s", r.status)
                    return []
                raw_text = await r.text(encoding="utf-8", errors="replace")

            raw_titles = re.findall(
                r"<title[^>]*>(.*?)</title>", raw_text, re.DOTALL | re.IGNORECASE
            )
            headlines: List[str] = []
            for t in raw_titles:
                clean = re.sub(r"<!\[CDATA\[|\]\]>|<[^>]*>", "", t).strip()
                if clean and "Cointelegraph" not in clean and len(clean) > 15:
                    headlines.append(clean)

            return headlines[:NEWS_MAX_HEADLINES]

        except Exception as exc:
            log.warning("[NewsWire] Fetch error — %s", exc)
            return []

    # ── VADER scoring ──────────────────────────────────────────────────────────
    @staticmethod
    def _vader_score(headlines: List[str]) -> Optional[float]:
        analyser = _get_vader()
        if analyser is None or not headlines:
            return None
        compounds = []
        for h in headlines:
            try:
                compounds.append(analyser.polarity_scores(h)["compound"])
            except Exception:
                continue
        if not compounds:
            return None
        return round(sum(compounds) / len(compounds), 4)

    # ── Refresh cycle ──────────────────────────────────────────────────────────
    async def _refresh(self):
        headlines = await self._fetch_rss()

        if not headlines:
            async with self._lock:
                if self._bias is not None:
                    return   # keep stale bias rather than going null
            async with self._lock:
                self._bias = MacroBias.neutral("rss_unavailable")
            return

        vader_score = self._vader_score(headlines)
        if vader_score is None:
            async with self._lock:
                self._bias = MacroBias.neutral("vader_unavailable")
            return

        # [P22-1] Optional OpenRouter enrichment
        llm_score:      Optional[float] = None
        llm_latency_ms: float           = 0.0
        llm_error:      Optional[str]   = None
        llm_enriched:   bool            = False

        if OPENROUTER_API_KEY:
            llm_score, llm_latency_ms, llm_error = await _openrouter_macro_score(
                headlines, self._session
            )
            llm_enriched = llm_score is not None

        # Blend: 60% VADER + 40% LLM if available
        if llm_score is not None:
            # LLM returns [-1,+1], VADER also [-1,+1] → direct blend
            final_score = round(0.60 * vader_score + 0.40 * llm_score, 4)
            source = f"openrouter+vader ({OPENROUTER_MODEL})"
        else:
            final_score = vader_score
            if llm_error:
                source = f"openrouter_failed+vader (err={llm_error})"
            else:
                source = "vader_only"

        label = MacroBias._label_from_score(final_score)
        bias  = MacroBias(
            score         = final_score,
            label         = label,
            headlines     = headlines[:5],
            source        = source,
            llm_enriched  = llm_enriched,
            llm_error     = llm_error,
            llm_latency_ms= round(llm_latency_ms, 2),
        )
        log.info(
            "[NewsWire] score=%.4f (%s) | vader=%.4f | llm=%s | n=%d",
            final_score, label,
            vader_score,
            f"{llm_score:.4f}" if llm_score is not None else "N/A",
            len(headlines),
        )
        async with self._lock:
            self._bias = bias

    # ── Refresh loop ───────────────────────────────────────────────────────────
    async def refresh_loop(self):
        log.info("[NewsWire] Loop: interval=%.0fs", NEWS_REFRESH_INTERVAL)
        while self._running:
            await asyncio.sleep(NEWS_REFRESH_INTERVAL)
            try:
                async with self._lock:
                    age = time.time() - (self._bias.ts if self._bias else 0.0)
                if age >= NEWS_CACHE_TTL:
                    await self._refresh()
            except Exception as exc:
                log.error("[NewsWire] Loop error: %s", exc)

    # ── Public API ─────────────────────────────────────────────────────────────
    async def get_macro_bias(self) -> MacroBias:
        async with self._lock:
            if self._bias is None:
                return MacroBias.neutral("not_yet_fetched")
            age = time.time() - self._bias.ts
            if age > NEWS_CACHE_TTL * 3:
                log.debug("[NewsWire] Data stale (%.0fs old)", age)
            return MacroBias(
                score          = self._bias.score,
                label          = self._bias.label,
                headlines      = list(self._bias.headlines),
                source         = self._bias.source,
                ts             = self._bias.ts,
                llm_enriched   = self._bias.llm_enriched,
                llm_error      = self._bias.llm_error,
                llm_latency_ms = self._bias.llm_latency_ms,
            )

    def is_bearish_block(self, bias: MacroBias) -> bool:
        return bias.score < NEWS_BEARISH_THRESHOLD
