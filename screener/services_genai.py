# screener/services_genai.py
import os, json
from typing import Dict, Any
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Strict JSON schema for structured output
STRAT_SCHEMA = {
    "name": "StrategyAdvice",
    "schema": {
        "type": "object",
        "properties": {
            "bias": {"type": "string", "enum": ["LONG", "SHORT", "NEUTRAL"]},
            "confidence": {"type": "integer", "minimum": 1, "maximum": 5},
            "rationale": {"type": "string"},
            "entry_plan": {
                "type": "object",
                "properties": {
                    "entry_type": {"type": "string", "enum": ["MARKET","LIMIT","STOP"]},
                    "entry_level": {"type": "number"},   # spot or option premium ref
                    "conditions": {"type": "string"}
                },
                "required": ["entry_type","entry_level"]
            },
            "stop_loss": {
                "type": "object",
                "properties": {
                    "stop_type": {"type": "string", "enum": ["PRICE","PREMIUM","ATR_MULTIPLE"]},
                    "stop_level": {"type": "number"},
                    "notes": {"type": "string"}
                },
                "required": ["stop_type","stop_level"]
            },
            "exit_targets": {
                "type": "array",
                "items": {"type": "object", "properties": {
                    "target_type":{"type":"string","enum":["PRICE","PREMIUM","RR_MULTIPLE"]},
                    "target_level":{"type":"number"},
                    "scale_out_pct":{"type":"number"}
                }, "required":["target_type","target_level","scale_out_pct"]},
                "minItems": 1
            },
            "risk_reward": {
                "type":"object",
                "properties":{
                    "est_rr": {"type":"number"},
                    "risk_per_trade_pct": {"type":"number"},
                    "position_size_hint": {"type":"string"}
                },
                "required":["est_rr"]
            },
            "options_strategy": {
                "type":"object",
                "properties":{
                    "name":{"type":"string"},
                    "legs":{"type":"array","items":{"type":"object","properties":{
                        "type":{"type":"string","enum":["BUY_CALL","SELL_CALL","BUY_PUT","SELL_PUT"]},
                        "strike":{"type":"number"},
                        "expiry_hint":{"type":"string"}
                    },"required":["type","strike"]}},
                    "why_this":{"type":"string"},
                    "max_loss_note":{"type":"string"},
                    "breakeven_hint":{"type":"string"}
                },
                "required":["name","legs","why_this"]
            },
            "risk_notes":{"type":"string"}
        },
        "required": ["bias","confidence","rationale","entry_plan","stop_loss","exit_targets","risk_reward","options_strategy","risk_notes"]
    }
}

SYSTEM_PROMPT = """You are an options strategist for Indian equities. 
Return ONLY valid JSON (no prose). 
Goal: Maximize profit and minimize loss responsibly.
Context provided includes last price, SMA200, distance from SMA200, ATR%, RSI, short-term and long-term slope, and event/news flags.

Rules:
- If sensitive event (earnings/board/dividend) within the window -> prefer limited-risk strategies or Neutral.
- Use the ATR%% to size stops/targets sensibly (e.g., 1.0–1.5x ATR for stops; 1.5–2.5x ATR for first target).
- Favor liquid, simple structures: bull/bear debit spread, credit spread, iron condor if range-bound.
- Produce stop-loss and at least one exit target.
- Keep strikes near sensible round levels (near spot or spot +/- ATR-based width).
- Avoid promises; include risk notes."""

def build_user_prompt(symbol: str, base: str, ctx: Dict[str, Any]) -> str:
    # ctx will have: close, sma200, distance_pct, signals{rsi, atr_pct, slope20, slope200}, 
    # in_nifty50, event_window_hit(bool), risk_per_trade_pct(float), capital(float), prefer_credit(bool)
    return json.dumps({
        "symbol": symbol,
        "ticker": base,
        "spot": ctx["close"],
        "sma200": ctx["sma200"],
        "distance_pct": ctx["distance_pct"],   # percent
        "signals": ctx["signals"],
        "in_nifty50": ctx.get("in_nifty50", False),
        "event_window_hit": ctx.get("event_window_hit", False),
        "risk_per_trade_pct": ctx.get("risk_per_trade_pct", 1.0),
        "capital": ctx.get("capital", None),
        "prefer_credit": ctx.get("prefer_credit", False),
        "expiry_hint": ctx.get("expiry_hint", "near-month"),
    }, ensure_ascii=False)

def ask_llm_for_strategy(symbol: str, base: str, ctx: Dict[str, Any], timeout: float = 20.0) -> Dict[str, Any]:
    """
    Returns a dict following STRAT_SCHEMA. Fails-soft to a minimal neutral plan if LLM errors.
    """
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.2,
            response_format={"type":"json_schema","json_schema":{"name":STRAT_SCHEMA["name"],"schema":STRAT_SCHEMA["schema"]}},
            messages=[
                {"role":"system","content": SYSTEM_PROMPT},
                {"role":"user","content": build_user_prompt(symbol, base, ctx)}
            ],
            timeout=timeout,
        )
        text = resp.choices[0].message.content
        return json.loads(text)
    except Exception:
        # Fail-soft minimal shape
        return {
            "bias":"NEUTRAL","confidence":2,
            "rationale":"Fallback: LLM unavailable.",
            "entry_plan":{"entry_type":"MARKET","entry_level":ctx["close"],"conditions":"None"},
            "stop_loss":{"stop_type":"ATR_MULTIPLE","stop_level":1.2,"notes":"Fallback"},
            "exit_targets":[{"target_type":"RR_MULTIPLE","target_level":2.0,"scale_out_pct":50}],
            "risk_reward":{"est_rr":1.5,"risk_per_trade_pct": ctx.get("risk_per_trade_pct",1.0), "position_size_hint":"Fallback"},
            "options_strategy":{"name":"Iron Condor","legs":[],"why_this":"Fallback","max_loss_note":"Fallback","breakeven_hint":"Fallback"},
            "risk_notes":"Use tiny size until LLM available."
        }

def llm_health():
    info = {"env_key_present": bool(os.getenv("OPENAI_API_KEY")), "models_ok": False, "chat_ok": False, "error": None}
    try:
        from openai import OpenAI
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        client.models.list()        # lightweight check
        info["models_ok"] = True
        client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":"ping"}],
            max_tokens=5,
            temperature=0.0,
            timeout=10,
        )
        info["chat_ok"] = True
    except Exception as e:
        info["error"] = str(e)[:400]
    return info