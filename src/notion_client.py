import os
import hashlib
import requests
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

class NotionError(Exception):
    pass

def make_app_key(company: str, role: str, url: str) -> str:
    s = (company.strip().lower() + "|" + role.strip().lower() + "|" + url.strip())
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(6),
       retry=retry_if_exception_type(NotionError))
def _post(url: str, payload: dict) -> dict:
    r = requests.post(url, headers=HEADERS, json=payload, timeout=30)
    if r.status_code == 429:
        raise NotionError("Rate limited (429)")
    if not r.ok:
        raise NotionError(f"Notion error {r.status_code}: {r.text}")
    return r.json()

@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(6),
       retry=retry_if_exception_type(NotionError))
def _patch(url: str, payload: dict) -> dict:
    r = requests.patch(url, headers=HEADERS, json=payload, timeout=30)
    if r.status_code == 429:
        raise NotionError("Rate limited (429)")
    if not r.ok:
        raise NotionError(f"Notion error {r.status_code}: {r.text}")
    return r.json()

def fetch_queued(limit: int = 5) -> list[dict]:
    payload = {
        "filter": {"property": "Status", "select": {"equals": "Not Applied"}},
        "page_size": min(max(limit, 1), 20),
        "sorts": [{"timestamp": "created_time", "direction": "ascending"}],
    }
    data = _post(f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query", payload)
    return data.get("results", [])

def update_page(page_id: str, properties: dict) -> None:
    payload = {"properties": properties}
    _patch(f"https://api.notion.com/v1/pages/{page_id}", payload)
