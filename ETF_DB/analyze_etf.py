# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "snowflake-connector-python",
#   "requests",
# ]
# ///
"""
ETF Data Analysis via Snowflake Cortex Analyst
----------------------------------------------
Sends natural language questions to Cortex Analyst using the semantic model
defined in analyst2_semantic_model.yaml (stored in @ETF_DB.LOCAL_COPY.cortex_stage).

Run with:
    uv run ETF_DB/analyze_etf.py
"""

import json
import pathlib
import sys
import tomllib

import requests
import snowflake.connector

SEMANTIC_MODEL_FILE = "@ETF_DB.LOCAL_COPY.cortex_stage/analyst2_semantic_model.yaml"

QUESTIONS = [
    "What are the top 10 ETF holdings by weight as of the latest date?",
    "Which ETF sectors have the highest total AUM?",
    "Show the top 5 ETFs by number of constituents.",
    "What is the average expense ratio by ETF category?",
    "Which constituent stocks appear in the most ETFs?",
]


def load_connection(connection_name: str = "snowconn") -> dict:
    config_path = pathlib.Path.home() / ".snowflake" / "config.toml"
    with open(config_path, "rb") as f:
        config = tomllib.load(f)
    name = config.get("default_connection_name", connection_name)
    return config["connections"][name]


def ask_cortex(url: str, token: str, question: str) -> None:
    print(f"\n{'=' * 64}")
    print(f"Q: {question}")
    print("=" * 64)

    payload = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": question}]}
        ],
        "semantic_model_file": SEMANTIC_MODEL_FILE,
    }
    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    if not resp.ok:
        print(f"  [Error {resp.status_code}] {resp.text}")
        return

    data = resp.json()
    content = data.get("message", {}).get("content", [])
    for block in content:
        if block["type"] == "text":
            print(f"\nAnswer:\n{block['text']}")
        elif block["type"] == "sql":
            print(f"\nGenerated SQL:\n{block['statement']}")
        elif block["type"] == "suggestions":
            print(f"\nSuggestions: {', '.join(block.get('suggestions', []))}")


def main() -> None:
    cfg = load_connection()

    conn = snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=cfg["password"],
        role=cfg.get("role", "ACCOUNTADMIN"),
        warehouse=cfg.get("warehouse", "cortex_analyst_wh"),
    )

    account = cfg["account"].lower()
    url = f"https://{account}.snowflakecomputing.com/api/v2/cortex/analyst/message"
    token = conn._rest.token  # session token for REST API auth

    print(f"Connected as {cfg['user']} | Account: {cfg['account']}")
    print(f"Semantic model: {SEMANTIC_MODEL_FILE}")

    for question in QUESTIONS:
        ask_cortex(url, token, question)

    conn.close()
    print("\n\nDone.")


if __name__ == "__main__":
    main()
