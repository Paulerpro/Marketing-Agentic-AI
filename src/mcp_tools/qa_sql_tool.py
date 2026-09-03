"""Natural-language Q&A over customer/product/transaction data.

PDF Workflow 2 (RouterAgent -> SQLAgent -> AnalysisAgent) collapsed into two Claude
calls: generate read-only SQL, validate it against an allow-list, execute it, then
narrate the result.
"""

from __future__ import annotations

import os
import re
from typing import Any

import pandas as pd
from anthropic import Anthropic
from sqlalchemy import text

from src.db.config import engine

ALLOWED_TABLES = {"clean_customers", "clean_products", "clean_transactions"}
FORBIDDEN_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|attach|exec|call|merge)\b",
    re.IGNORECASE,
)

SQL_SYSTEM_PROMPT = """You are a read-only SQL analyst for a PostgreSQL marketing database \
with exactly these tables:

clean_customers(customer_id, email, name, age, gender, country, city, phone_number, \
interests, signup_date, last_purchase_date, total_spent, purchase_frequency, churn)
clean_products(product_id, product_name, category, description, price, stock_status)
clean_transactions(transaction_id, customer_id, product_id, total_price, quantity, \
purchase_date)

Rules:
- Respond with ONLY a single PostgreSQL SELECT statement, nothing else - no prose, no \
markdown code fences, no trailing semicolon.
- Never write or alter data, and never reference any table other than the three listed \
above.
- Prefer explicit column lists over SELECT *. Add LIMIT 200 unless the question clearly \
asks for an aggregate.
"""

# Overridable so a cost-conscious deployment can point this at a cheaper model without
# touching code. Defaults to Anthropic's current recommended model.
LLM_MODEL = os.getenv("QA_MODEL", "claude-opus-5")


def _strip_fence(sql: str) -> str:
    sql = sql.strip()
    if sql.startswith("```"):
        sql = sql.strip("`")
        sql = sql.split("\n", 1)[-1] if "\n" in sql else sql
    return sql.strip().rstrip(";").strip()


def _generate_sql(question: str) -> str:
    client = Anthropic()
    response = client.messages.create(
        model=LLM_MODEL,
        max_tokens=400,
        system=SQL_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": question}],
    )
    raw = "".join(b.text for b in response.content if b.type == "text")
    return _strip_fence(raw)


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


def _summarize(question: str, result_df: pd.DataFrame) -> str:
    client = Anthropic()
    preview = result_df.head(20).to_dict(orient="records")
    response = client.messages.create(
        model=LLM_MODEL,
        max_tokens=500,
        system=(
            "Summarize SQL query results for a marketing analyst in 2-4 sentences. "
            "Call out concrete numbers and any notable trend or outlier. No preamble."
        ),
        messages=[
            {
                "role": "user",
                "content": (
                    f"Question: {question}\n\n"
                    f"Result rows ({len(result_df)} total, showing up to 20):\n{preview}"
                ),
            }
        ],
    )
    return "".join(b.text for b in response.content if b.type == "text").strip()


def answer_customer_question(question: str) -> dict[str, Any]:
    """Run one NL question through generate -> validate -> execute -> narrate."""
    sql = _generate_sql(question)
    _validate_sql(sql)

    with engine.connect() as conn:
        result_df = pd.read_sql(text(sql), conn)

    narrative = (
        _summarize(question, result_df) if not result_df.empty else "No matching rows found."
    )

    return {
        "question": question,
        "sql": sql,
        "row_count": len(result_df),
        "rows": result_df.head(50).to_dict(orient="records"),
        "narrative": narrative,
    }
