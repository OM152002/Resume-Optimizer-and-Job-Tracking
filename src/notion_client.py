import os
import re
import hashlib
import requests
from typing import Any, Dict, Tuple, Optional
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type


class NotionError(Exception):
    pass


class NotionRetryableError(NotionError):
    pass


def _clean_env_value(value: str) -> str:
    value = (value or "").strip()
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        value = value[1:-1].strip()
    return value


def _get_env_required(name: str) -> str:
    value = _clean_env_value(os.environ.get(name, ""))
    if not value:
        raise NotionError(f"Missing required environment variable: {name}")
    return value


def _get_notion_token() -> str:
    token = _get_env_required("NOTION_TOKEN")
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token


def _get_notion_db_id() -> str:
    return _get_env_required("NOTION_DB_ID")


def _get_headers() -> dict:
    return {
        "Authorization": f"Bearer {_get_notion_token()}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def _raise_for_status(r: requests.Response) -> None:
    if r.status_code == 429:
        raise NotionRetryableError("Rate limited (429)")
    if 500 <= r.status_code < 600:
        raise NotionRetryableError(f"Notion server error {r.status_code}: {r.text}")
    if not r.ok:
        if r.status_code == 401:
            raise NotionError("Notion unauthorized (401). Check NOTION_TOKEN.")
        raise NotionError(f"Notion error {r.status_code}: {r.text}")

def normalize_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

@retry(
    wait=wait_exponential(min=1, max=20),
    stop=stop_after_attempt(6),
    retry=retry_if_exception_type(NotionRetryableError),
)
def _get(url: str) -> dict:
    r = requests.get(url, headers=_get_headers(), timeout=30)
    _raise_for_status(r)
    return r.json()

@retry(
    wait=wait_exponential(min=1, max=20),
    stop=stop_after_attempt(6),
    retry=retry_if_exception_type(NotionRetryableError),
)
def _post(url: str, payload: dict) -> dict:
    r = requests.post(url, headers=_get_headers(), json=payload, timeout=30)
    _raise_for_status(r)
    return r.json()

@retry(
    wait=wait_exponential(min=1, max=20),
    stop=stop_after_attempt(6),
    retry=retry_if_exception_type(NotionRetryableError),
)
def _patch(url: str, payload: dict) -> dict:
    r = requests.patch(url, headers=_get_headers(), json=payload, timeout=30)
    _raise_for_status(r)
    return r.json()

def get_database_schema() -> dict:
    return _get(f"https://api.notion.com/v1/databases/{_get_notion_db_id()}")

def build_property_index(schema: dict) -> Dict[str, Tuple[str, dict]]:
    props = schema.get("properties", {}) or {}
    idx: Dict[str, Tuple[str, dict]] = {}
    for actual_name, prop_schema in props.items():
        idx[normalize_name(actual_name)] = (actual_name, prop_schema)
    return idx

def resolve_prop(idx: Dict[str, Tuple[str, dict]], name: str) -> Optional[Tuple[str, dict]]:
    return idx.get(normalize_name(name))

def set_prop_value(prop_schema: dict, value: Any) -> dict:
    ptype = prop_schema.get("type")

    if ptype == "title":
        return {"title": [{"text": {"content": str(value)}}]}

    if ptype == "rich_text":
        return {"rich_text": [{"text": {"content": str(value)}}]}

    if ptype == "url":
        return {"url": str(value) if value else None}

    if ptype == "number":
        return {"number": None if value is None else float(value)}

    if ptype == "select":
        return {"select": {"name": str(value)}}

    if ptype == "status":
        return {"status": {"name": str(value)}}

    raise ValueError(f"Unsupported property type: {ptype}")

def update_page_safe(page_id: str, desired: Dict[str, Any], idx: Dict[str, Tuple[str, dict]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"updated": [], "skipped": []}
    props_payload: Dict[str, Any] = {}

    for desired_name, desired_value in desired.items():
        resolved = resolve_prop(idx, desired_name)
        if not resolved:
            out["skipped"].append({"property": desired_name, "reason": "missing_in_db"})
            continue

        actual_name, prop_schema = resolved
        try:
            props_payload[actual_name] = set_prop_value(prop_schema, desired_value)
            out["updated"].append(actual_name)
        except Exception as e:
            out["skipped"].append({"property": actual_name, "reason": str(e)})

    if not props_payload:
        return out

    _patch(f"https://api.notion.com/v1/pages/{page_id}", {"properties": props_payload})
    return out

def fetch_by_status(status_name: str, limit: int, idx: Dict[str, Tuple[str, dict]]) -> list[dict]:
    resolved = resolve_prop(idx, "Status")
    if not resolved:
        raise NotionError("Database has no 'Status' property")

    actual_name, prop_schema = resolved
    ptype = prop_schema.get("type")

    if ptype == "status":
        filter_obj = {"property": actual_name, "status": {"equals": status_name}}
    elif ptype == "select":
        filter_obj = {"property": actual_name, "select": {"equals": status_name}}
    else:
        raise NotionError(f"'Status' property type is {ptype}, expected status/select")

    payload = {
        "filter": filter_obj,
        "page_size": min(max(limit, 1), 20),
        "sorts": [{"timestamp": "created_time", "direction": "ascending"}],
    }
    data = _post(
        f"https://api.notion.com/v1/databases/{_get_notion_db_id()}/query", payload
    )
    return data.get("results", [])
