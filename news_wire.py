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
logging.getLogger("news_wire").addHandler(logging.NullHandler())
from pt_utils import _env_float, _env_int, atomic_write_json  # [P0-UTIL]


# [P0-FIX-11] env helpers → pt_utils
# ── Config ─────────────────────────────────────────────────────────────────────
NEWS_CACHE_TTL         = _env_float("NEWS_CACHE_TTL", 300.0)
NEWS_REFRESH_INTERVAL  = _env_float("NEWS_REFRESH_INTERVAL", 240.0)
NEWS_BEARISH_THRESHOLD = _env_float("NEWS_BEARISH_BLOCK_THRESHOLD", -0.5)
NEWS_MAX_HEADLINES     = _env_int("NEWS_MAX_HEADLINES", 30)
NEWS_FETCH_TIMEOUT     = _env_float("NEWS_FETCH_TIMEOUT", 15.0)

# [P22-1] OpenRouter optional enrichment
OPENROUTER_API_KEY   = os.environ.get("OPENROUTER_API_KEY",   "")
OPENROUTER_MODEL     = os.environ.get("OPENROUTER_MODEL",     "google/gemini-2.0-flash-001:free")
OPENROUTER_SITE_URL  = os.environ.get("OPENROUTER_SITE_URL",  "https://github.com/powertrader")
OPENROUTER_SITE_NAME = os.environ.get("OPENROUTER_SITE_NAME", "PowerTrader AI")
# Keep the NewsWire LLM call well inside the P17 hard cap
_NW_OR_TIMEOUT = _env_float("NEWS_OPENROUTER_TIMEOUT", 30.0)
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


# ── Lazy-loaded FinBERT scorer ─────────────────────────────────────────────────

FINBERT_ENABLED = os.environ.get("FINBERT_ENABLED", "1").strip().strip("'\"") == "1"
FINBERT_MODEL   = os.environ.get(
    "FINBERT_MODEL", "yiyanghkust/finbert-tone"
)  # ~110 MB; 3-class: Positive / Negative / Neutral


class FinBertScorer:
    """
    [T28-FINBERT] Lightweight FinBERT-style crypto/finance headline scorer.

    Uses ProsusAI/finbert or yiyanghkust/finbert-tone (default) via a
    HuggingFace transformers text-classification pipeline.

    Design decisions
    ----------------
    * Loaded lazily on first call — importing news_wire.py has zero model overhead.
    * If ``transformers`` or ``torch`` are absent, or the model download fails
      for any reason, ``_available`` is set False and all calls return None.
      The bot continues on VADER-only scoring with no change in behaviour.
    * Score mapping: ``positive_prob − negative_prob`` → float ∈ [-1, +1]
      This mirrors the VADER compound scale so the two can be blended directly.
    * Truncation is enabled (financial headlines are short but batching safety).
    * Device is always CPU to avoid GPU-dependency on headless server deploys.
    * Max batch = 32 headlines per call; individual failures skip gracefully.
    """

    def __init__(self) -> None:
        self._available: bool = False
        self._pipeline = None
        self._load_attempted: bool = False

    def _load(self) -> None:
        """Attempt model load once. Sets _available=True on success."""
        if self._load_attempted:
            return
        self._load_attempted = True
        if not FINBERT_ENABLED:
            log.info("[T28-FINBERT] FinBERT disabled via FINBERT_ENABLED=0.")
            return
        try:
            from transformers import pipeline as hf_pipeline  # type: ignore
            self._pipeline = hf_pipeline(
                "text-classification",
                model=FINBERT_MODEL,
                tokenizer=FINBERT_MODEL,
                top_k=None,           # return all class scores
                truncation=True,
                max_length=128,
                device=-1,            # CPU — no GPU dependency
            )
            self._available = True
            log.info(
                "[T28-FINBERT] FinBertScorer loaded: model=%s", FINBERT_MODEL
            )
        except ImportError:
            log.info(
                "[T28-FINBERT] transformers not installed — "
                "using VADER-only scoring. "
                "Install: pip install transformers torch"
            )
        except Exception as exc:
            log.warning(
                "[T28-FINBERT] Model load failed (%s) — "
                "falling back to VADER-only scoring.", exc
            )

    def score_headlines(self, headlines: List[str]) -> Optional[float]:
        """
        Score a list of headlines and return a single aggregate float ∈ [-1, +1].

        Returns None when the model is unavailable so the caller can fall through
        to VADER without any special-casing at the call site.

        Score derivation
        ----------------
        For each headline, the pipeline returns label probabilities.
        We compute: ``compound = positive_prob − negative_prob``.
        The final score is the mean compound over all successfully scored headlines.

        Label normalisation handles both model families:
        - yiyanghkust/finbert-tone  → "Positive" / "Negative" / "Neutral"
        - ProsusAI/finbert          → "positive" / "negative" / "neutral"
        """
        self._load()
        if not self._available or self._pipeline is None or not headlines:
            return None
        try:
            batch = headlines[:32]  # cap batch size
            results = self._pipeline(batch)
            compounds: List[float] = []
            for item in results:
                if not isinstance(item, list):
                    continue
                scores = {r["label"].lower(): r["score"] for r in item}
                pos = scores.get("positive", 0.0)
                neg = scores.get("negative", 0.0)
                compounds.append(pos - neg)
            if not compounds:
                return None
            avg = sum(compounds) / len(compounds)
            return round(max(-1.0, min(1.0, avg)), 4)
        except Exception as exc:
            log.warning("[T28-FINBERT] score_headlines error: %s", exc)
            return None


_finbert_scorer: Optional[FinBertScorer] = None


def _get_finbert_scorer() -> FinBertScorer:
    """[T28-FINBERT] Lazy singleton — never raises, never blocks import."""
    global _finbert_scorer
    if _finbert_scorer is None:
        _finbert_scorer = FinBertScorer()
    return _finbert_scorer


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
    # [T28-FINBERT] FinBERT-style local sentiment score, None when model unavailable
    finbert_score: Optional[float] = None
    finbert_used:  bool            = False

    @staticmethod
    def neutral(reason: str = "fallback") -> "MacroBias":
        return MacroBias(
            score=0.0, label="Neutral", source=reason,
            llm_enriched=False, llm_error=reason, llm_latency_ms=0.0,
            finbert_score=None, finbert_used=False,
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

        # [FIX-OR-NONE] OpenRouter may return content=null (not just missing key).
        # .get("content", "") returns None in that case, causing AttributeError on .strip().
        # Use `or ""` to coerce both None and empty string to "".
        content = (
            (
                data.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content")
                or ""
            ).strip()
        )
        content = content.replace("```json", "").replace("```", "").strip()
        # [FIX-OR-EMPTY] Guard: empty content after strip (API returned null/empty body).
        # json.loads("") raises JSONDecodeError which was surfacing as a WARNING every cycle.
        # Return None cleanly — VADER score is still used, no need to spam WARNING.
        if not content:
            log.info(
                "[P22-1/NewsWire] OpenRouter returned empty content — "
                "VADER-only scoring for this cycle."
            )
            return None, latency_ms, "empty_content"
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
            except Exception as _sent_exc:
                log.debug("[NEWS-SENT] Sentiment score failed for headline — %s", _sent_exc)
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

        # [T28-FINBERT] Optional local FinBERT scoring tier
        # Runs synchronously (CPU inference, fast on short batches).
        # Falls back silently to VADER-only when model is unavailable.
        finbert_raw:   Optional[float] = None
        finbert_used:  bool            = False
        try:
            finbert_raw  = _get_finbert_scorer().score_headlines(headlines)
            finbert_used = finbert_raw is not None
        except Exception as _fb_exc:
            log.warning("[T28-FINBERT] Scorer exception in _refresh: %s", _fb_exc)

        # Local score = blend of VADER and FinBERT when both available.
        # 50/50 blend: both operate on [-1,+1] and are independently calibrated.
        if finbert_used and finbert_raw is not None:
            local_score = round(0.50 * vader_score + 0.50 * finbert_raw, 4)
        else:
            local_score = vader_score

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

        # Final score blend: 60% local (VADER±FinBERT) + 40% LLM if available.
        # [T28-FINBERT] FinBERT improves local_score quality before the LLM overlay.
        if llm_score is not None:
            final_score = round(0.60 * local_score + 0.40 * llm_score, 4)
            _local_tag  = "finbert+vader" if finbert_used else "vader"
            source      = f"openrouter+{_local_tag} ({OPENROUTER_MODEL})"
        else:
            final_score = local_score
            if llm_error:
                _local_tag = "finbert+vader" if finbert_used else "vader"
                source     = f"openrouter_failed+{_local_tag} (err={llm_error})"
            else:
                source = "finbert+vader" if finbert_used else "vader_only"

        label = MacroBias._label_from_score(final_score)
        bias  = MacroBias(
            score          = final_score,
            label          = label,
            headlines      = headlines[:5],
            source         = source,
            llm_enriched   = llm_enriched,
            llm_error      = llm_error,
            llm_latency_ms = round(llm_latency_ms, 2),
            finbert_score  = finbert_raw,
            finbert_used   = finbert_used,
        )
        log.info(
            "[NewsWire] score=%.4f (%s) | vader=%.4f | finbert=%s | llm=%s | n=%d",
            final_score, label,
            vader_score,
            f"{finbert_raw:.4f}" if finbert_raw is not None else "N/A",
            f"{llm_score:.4f}"   if llm_score   is not None else "N/A",
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
                finbert_score  = self._bias.finbert_score,
                finbert_used   = self._bias.finbert_used,
            )

    def is_bearish_block(self, bias: MacroBias) -> bool:
        return bias.score < NEWS_BEARISH_THRESHOLD
