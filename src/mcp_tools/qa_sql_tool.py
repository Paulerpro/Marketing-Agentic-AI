"""Natural-language Q&A over customer/product/transaction data.

PDF Workflow 2 (RouterAgent -> SQLAgent -> AnalysisAgent) collapsed into two LLM
calls: generate read-only SQL, validate it against an allow-list, execute it, then
narrate the result. Provider (Claude or Gemini) is picked by src.utils.llm_provider
based on which API key is set.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd
from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy import text

from src.db.config import engine
from src.utils.llm_provider import extract_text, get_chat_model

ALLOWED_TABLES = {"clean_customers", "clean_products", "clean_transactions", "churn_scores"}
FORBIDDEN_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|attach|exec|call|merge)\b",
    re.IGNORECASE,
)

SQL_SYSTEM_PROMPT = """You are a read-only SQL analyst for a PostgreSQL marketing database \
with exactly these tables and column types:

clean_customers(customer_id VARCHAR, email VARCHAR, name VARCHAR, age INTEGER, \
gender VARCHAR, country VARCHAR, city VARCHAR, phone_number VARCHAR, interests VARCHAR, \
signup_date DATE, last_purchase_date DATE, total_spent FLOAT, purchase_frequency FLOAT, \
churn INTEGER)
clean_products(product_id VARCHAR, product_name VARCHAR, category VARCHAR, \
description VARCHAR, price FLOAT, stock_status VARCHAR)
clean_transactions(transaction_id VARCHAR, customer_id VARCHAR, product_id VARCHAR, \
total_price FLOAT, quantity INTEGER, purchase_date TIMESTAMP)
churn_scores(customer_id VARCHAR, score FLOAT, risk_label VARCHAR, run_date TIMESTAMP, \
model_version VARCHAR)

You may be given recent conversation history before the current question - use it to
resolve pronouns and references ("him", "her", "that customer", "it") to the specific
entity they mean, and to understand follow-ups that build on a prior answer.

Rules:
- If the message is NOT a question about this data - a greeting, small talk, a request \
for something outside these tables, anything you can't answer with a SELECT against \
them, EVEN AFTER using conversation history to resolve references - respond with \
exactly the single word NOT_A_DATA_QUESTION and nothing else. Do not invent a \
plausible-looking query for an unrelated or conversational message; a default/fallback \
SELECT is worse than admitting you can't answer it. But a follow-up like "what should \
we do about him" IS answerable once history tells you who "him" is - write a query \
that pulls the relevant data (e.g. their current churn_scores row) so the answer can \
be grounded in real numbers, rather than rejecting it.
- Otherwise, respond with ONLY a single PostgreSQL SELECT statement, nothing else - no \
prose, no markdown code fences, no trailing semicolon.
- Never write or alter data, and never reference any table other than the ones listed \
above.
- Prefer explicit column lists over SELECT *. Add LIMIT 200 unless the question clearly \
asks for an aggregate.
- churn is INTEGER (0 = active, 1 = churned), NOT boolean. Compare it with `churn = 1` \
or `churn = 0` - never `IS TRUE`, `IS FALSE`, or a bare `WHERE churn`, all of which \
PostgreSQL rejects on an integer column.
- Every other column above is exactly the type listed - don't guess a different type \
for any of them (e.g. don't treat stock_status as boolean, or price as integer).
- churn (on clean_customers) and churn_scores.score/.risk_label are DIFFERENT facts. \
churn is a static historical label ("did they ever officially churn"). churn_scores \
holds the current CatBoost model's live prediction (score 0-1, risk_label \
low/medium/high) and is one row per customer, refreshed on each scoring run - it's \
the answer for any question about churn RISK, PROBABILITY, or PREDICTION. If a \
question is ambiguous about which one it means, prefer churn_scores and join to \
clean_customers on customer_id for name/email context.
- All text values (name, email, city, country, category, product_name, etc.) are \
stored lowercase. String comparisons must be case-insensitive - use ILIKE or wrap \
both sides in LOWER(...), never a case-sensitive `=` on text.
"""


def _require_llm(max_output_tokens: int):
    llm = get_chat_model(max_output_tokens=max_output_tokens, model_env_var="QA_MODEL")
    if llm is None:
        raise RuntimeError(
            "No LLM configured - set ANTHROPIC_API_KEY or GEMINI_API_KEY to use Customer Q&A."
        )
    return llm


def _strip_fence(sql: str) -> str:
    sql = sql.strip()
    if sql.startswith("```"):
        sql = sql.strip("`")
        sql = sql.split("\n", 1)[-1] if "\n" in sql else sql
    return sql.strip().rstrip(";").strip()


def _history_block(history: list[dict[str, Any]] | None, max_turns: int = 5) -> str:
    """Recent turns as plain text context, not real multi-turn messages - the model's
    output format for this task is fixed (SQL or the sentinel), so history is framed
    as reference material rather than something to reply "in character" to."""
    if not history:
        return ""
    recent = history[-max_turns:]
    lines = [f"Q: {h.get('question', '')}\nA: {h.get('narrative', '')}" for h in recent]
    return (
        "Recent conversation for context (pronouns like 'him'/'her'/'it'/'that "
        "customer' may refer to entities mentioned here):\n"
        + "\n\n".join(lines)
        + "\n\n---\n\n"
    )


def _generate_sql(question: str, history: list[dict[str, Any]] | None = None) -> str:
    # 2048, not something smaller: Gemini's 3.x models spend part of the token budget
    # on internal reasoning, so a tight budget here truncates the SQL mid-query and
    # _validate_sql correctly (but confusingly) rejects it as "no known table".
    llm = _require_llm(max_output_tokens=2048)
    prompt = _history_block(history) + f"Current question: {question}"
    response = llm.invoke([SystemMessage(content=SQL_SYSTEM_PROMPT), HumanMessage(content=prompt)])
    return _strip_fence(extract_text(response.content))


def _validate_sql(sql: str) -> None:
    lowered = sql.lower()
    if not lowered.startswith("select"):
        raise ValueError(f"Generated query is not a SELECT statement: {sql!r}")
    if ";" in sql:
        raise ValueError("Multiple statements are not allowed")
    if FORBIDDEN_KEYWORDS.search(sql):
        raise ValueError(f"Query contains a forbidden keyword: {sql!r}")
    if not any(t in lowered for t in ALLOWED_TABLES):
        raise ValueError(f"Query does not reference any known table: {sql!r}")


def _summarize(question: str, result_df: pd.DataFrame, history: list[dict[str, Any]] | None = None) -> str:
    llm = _require_llm(max_output_tokens=2048)
    preview = result_df.head(20).to_dict(orient="records")
    prompt = (
        _history_block(history)
        + f"Current question: {question}\n\n"
        + f"Result rows ({len(result_df)} total, showing up to 20):\n{preview}"
    )
    response = llm.invoke(
        [
            SystemMessage(
                content=(
                    "Summarize SQL query results for a marketing analyst in 2-4 sentences. "
                    "Call out concrete numbers and any notable trend or outlier. If the "
                    "question explicitly asks for advice, a recommendation, or what to do, "
                    "give one concrete suggestion grounded in the numbers you found (e.g. "
                    "'given the 90% churn risk and no purchases since March, consider a "
                    "targeted retention offer') - only when asked, don't editorialize "
                    "otherwise. No preamble."
                )
            ),
            HumanMessage(content=prompt),
        ]
    )
    return extract_text(response.content).strip()


def answer_customer_question(question: str, history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Run one NL question through generate -> validate -> execute -> narrate.

    history: recent turns as [{"question": ..., "narrative": ...}, ...], oldest first -
    lets follow-ups ("what should we do about him") resolve references to a prior turn
    instead of being answered (or rejected) with no memory of the conversation.
    """
    sql = _generate_sql(question, history)

    if sql.strip().upper() == "NOT_A_DATA_QUESTION":
        return {
            "question": question,
            "sql": None,
            "row_count": 0,
            "rows": [],
            "narrative": (
                "I can only answer questions about your customer, product, transaction, "
                "and churn-score data - that wasn't one of those. Try something like "
                "\"which customers are inactive for 60+ days?\" or \"what's the churn "
                "risk for CUST0001?\""
            ),
        }

    _validate_sql(sql)

    with engine.connect() as conn:
        result_df = pd.read_sql(text(sql), conn)

    narrative = (
        _summarize(question, result_df, history) if not result_df.empty else "No matching rows found."
    )

    return {
        "question": question,
        "sql": sql,
        "row_count": len(result_df),
        "rows": result_df.head(50).to_dict(orient="records"),
        "narrative": narrative,
    }
