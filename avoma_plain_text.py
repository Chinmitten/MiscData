#This is the one from Cursor

#!/usr/bin/env python3
"""
HubSpot List -> Avoma Transcript (Plain Text) -> Webhook
Optimized: fetch Avoma meetings ONCE per owner, accept meetings where the owner is organizer OR attendee.

Required env:
  HUBSPOT_TOKEN   = HubSpot Private App token (crm.lists.read, crm.objects.contacts.read, settings.users.read)
  HUBSPOT_LIST_ID = HubSpot list ID (v3 Lists)
  AVOMA_API_KEY   = Avoma API key (meetings.read, transcripts.read)
  WEBHOOK_URL     = Destination webhook URL

CLI:
  --only-email you@example.com
  --days 365
  --limit 100
  --pages 5
  --delay 0.5
"""

import os
import sys
import json
import time
import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from collections import defaultdict

import requests

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s - %(levelname)s - %(message)s",
  handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("hs-avoma")

def http_request(method: str, url: str, headers: Dict[str, str], timeout: int = 45, retries: int = 5, backoff_start: float = 0.7, **kwargs) -> Optional[requests.Response]:
  sleep = backoff_start
  for _ in range(max(1, retries)):
    try:
      r = requests.request(method, url, headers=headers, timeout=timeout, **kwargs)
      if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(sleep)
        sleep = min(sleep * 2, 12.0)
        continue
      return r
    except Exception as e:
      log.warning(f"{method} {url} -> exception {type(e).__name__}: {e}")
      time.sleep(sleep)
      sleep = min(sleep * 2, 12.0)
  return None

HS_BASE = "https://api.hubapi.com"
def hs_headers(token: str) -> Dict[str, str]:
  return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def hs_get_list_member_ids(token: str, list_id: str, delay: float) -> List[str]:
  r = http_request("GET", f"{HS_BASE}/crm/v3/lists/{list_id}", hs_headers(token))
  if not r or r.status_code == 404:
    raise RuntimeError("HubSpot list not found")
  r.raise_for_status()
  ids: List[str] = []
  url = f"{HS_BASE}/crm/v3/lists/{list_id}/memberships"
  params = {"limit": 100}
  after: Optional[str] = None
  while True:
    if after:
      params["after"] = after
    rr = http_request("GET", url, hs_headers(token), params=params)
    if not rr or rr.status_code == 404:
      break
    rr.raise_for_status()
    data = rr.json() or {}
    for it in data.get("results", []):
      rid = it.get("recordId")
      if rid:
        ids.append(str(rid))
    after = data.get("paging", {}).get("next", {}).get("after")
    if not after:
      break
    time.sleep(delay)
  log.info(f"HubSpot: collected {len(ids)} contact IDs from list {list_id}")
  return ids

def hs_batch_read_contacts(token: str, ids: List[str], delay: float) -> List[Dict[str, Any]]:
  if not ids:
    return []
  props = ["email", "hubspot_owner_id", "firstname", "lastname"]
  url = f"{HS_BASE}/crm/v3/objects/contacts/batch/read"
  out: List[Dict[str, Any]] = []
  for i in range(0, len(ids), 100):
    chunk = ids[i:i+100]
    payload = {"properties": props, "inputs": [{"id": x} for x in chunk]}
    r = http_request("POST", url, hs_headers(token), json=payload)
    if not r:
      break
    r.raise_for_status()
    out.extend(r.json().get("results", []))
    time.sleep(delay)
  log.info(f"HubSpot: batch-read {len(out)} contacts")
  return out

def hs_owner_email_map(token: str, owner_ids: List[str], delay: float) -> Dict[str, str]:
  mapping: Dict[str, str] = {}
  unique_ids = sorted(set([oid for oid in owner_ids if oid]))
  for oid in unique_ids:
    r = http_request("GET", f"{HS_BASE}/crm/v3/owners/{oid}", hs_headers(token))
    if not r or r.status_code == 404:
      continue
    try:
      r.raise_for_status()
      data = r.json() or {}
      email = (data.get("email") or "").strip().lower()
      if email:
        mapping[str(data.get("id") or oid)] = email
    except Exception:
      pass
    time.sleep(delay)
  log.info(f"HubSpot: resolved {len(mapping)}/{len(unique_ids)} owner emails")
  return mapping

AV_BASE = "https://api.avoma.com"
def av_headers(key: str) -> Dict[str, str]:
  return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

def av_list_meetings_by_organizer(avoma_key: str, organizer_email: str, frm: str, to: str, limit: int, pages: int, delay: float) -> List[Dict[str, Any]]:
  items: List[Dict[str, Any]] = []
  url = f"{AV_BASE}/v1/meetings"
  tried = 0
  for params in (
    {"organizer_email": organizer_email, "from_date": frm, "to_date": to, "limit": limit},
    {"organizerEmail": organizer_email, "from_date": frm, "to_date": to, "limit": limit},
  ):
    tried += 1
    nxt: Optional[str] = None
    for _ in range(max(1, pages)):
      r = http_request("GET", nxt or url, av_headers(avoma_key), params=None if nxt else params)
      if not r:
        break
      if r.status_code in (400, 404):
        break
      r.raise_for_status()
      data = r.json() or {}
      chunk = data.get("results") or data.get("meetings") or []
      if not isinstance(chunk, list):
        break
      items.extend(chunk)
      nxt = data.get("next")
      if not nxt:
        break
      time.sleep(delay)
    if items:
      break
  log.info(f"Avoma: fetched {len(items)} meetings for organizer {organizer_email} (tried {tried} param styles)")
  return items

def av_fetch_transcript_json(avoma_key: str, meeting_id: str, transcription_uuid: Optional[str]) -> Optional[Dict[str, Any]]:
  r = http_request("GET", f"{AV_BASE}/v1/meetings/{meeting_id}/transcript", av_headers(avoma_key), params={"format": "json"})
  if r and r.status_code == 200:
    return r.json()
  if transcription_uuid:
    r2 = http_request("GET", f"{AV_BASE}/v1/transcriptions/{transcription_uuid}", av_headers(avoma_key))
    if r2 and r2.status_code == 200:
      return r2.json()
  r3 = http_request("GET", f"{AV_BASE}/v1/meetings/{meeting_id}/transcriptions", av_headers(avoma_key))
  if r3 and r3.status_code == 200:
    data = r3.json()
    items = data.get("results", data) if isinstance(data, dict) else data
    if isinstance(items, list) and items:
      return items[0]
  return None

def transcript_json_to_text(t: Any) -> str:
  if not t:
    return ""
  if isinstance(t, dict):
    for k in ("content", "text", "transcript", "body"):
      v = t.get(k)
      if isinstance(v, str) and v.strip():
        return v.strip()
    if isinstance(t.get("transcript"), list):
      speakers: Dict[Any, str] = {}
      for s in t.get("speakers") or []:
        sid = s.get("id")
        name = s.get("name") or s.get("speaker") or (f"Speaker {sid}" if sid is not None else "")
        if sid is not None:
          speakers[sid] = name
      lines: List[str] = []
      for tr in t["transcript"]:
        if not isinstance(tr, dict):
          continue
        txt = (tr.get("transcript") or tr.get("text") or "").strip()
        if not txt:
          continue
        spk = speakers.get(tr.get("speaker_id")) or ""
        lines.append(f"{spk + ': ' if spk else ''}{txt}")
      if lines:
        return "\n".join(lines).strip()
    lines2: List[str] = []
    for k in ("paragraphs", "segments"):
      for it in t.get(k) or []:
        spk = (it.get("speaker") or it.get("speaker_label") or "").strip()
        txt = (it.get("text") or "").strip()
        if txt:
          lines2.append(f"{spk + ': ' if spk else ''}{txt}")
    if lines2:
      return "\n".join(lines2).strip()
    if isinstance(t.get("results"), list):
      parts = [transcript_json_to_text(x) for x in t["results"]]
      parts = [p for p in parts if p]
      if parts:
        return "\n".join(parts).strip()
  if isinstance(t, list):
    parts = [transcript_json_to_text(x) for x in t]
    parts = [p for p in parts if p]
    return "\n".join(parts).strip()
  if isinstance(t, str):
    return t.strip()
  return ""

def parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
  if not dt_str:
    return None
  try:
    s = dt_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if not dt.tzinfo:
      dt = dt.replace(tzinfo=timezone.utc)
    return dt
  except Exception:
    return None

def looks_like_call(m: Dict[str, Any]) -> bool:
  return bool(
    m.get("transcript_ready") or
    m.get("transcription_uuid") or
    m.get("audio_ready") or
    m.get("video_ready") or
    (m.get("processing_status") in {"transcription_available", "completed"})
  )

def attendees_emails(m: Dict[str, Any]) -> List[str]:
  out: List[str] = []
  for a in m.get("attendees") or []:
    e = (a.get("email") or "").strip().lower()
    if e:
      out.append(e)
  return sorted(set(out))

def run_pipeline(
  hubspot_token: str,
  hubspot_list_id: str,
  avoma_api_key: str,
  webhook_url: str,
  only_emails: Optional[List[str]],
  days: int,
  limit: int,
  pages: int,
  delay: float,
) -> Dict[str, Any]:
  results = {"contacts_processed": 0, "emails_found": 0, "meetings_found": 0, "transcripts_posted": 0, "errors": []}

  ids = hs_get_list_member_ids(hubspot_token, hubspot_list_id, delay)
  results["contacts_processed"] = len(ids)
  if not ids:
    return results

  contacts = hs_batch_read_contacts(hubspot_token, ids, delay)
  owner_ids = list({
    (c.get("properties") or {}).get("hubspot_owner_id")
    for c in contacts
    if (c.get("properties") or {}).get("hubspot_owner_id")
  })
  owner_email_by_id = hs_owner_email_map(hubspot_token, owner_ids, delay)

  cust_to_owner: Dict[str, str] = {}
  for c in contacts:
    props = c.get("properties") or {}
    ce = (props.get("email") or "").strip().lower()
    oid = (props.get("hubspot_owner_id") or "").strip()
    if not ce:
      continue
    oe = owner_email_by_id.get(oid)
    if oe:
      cust_to_owner[ce] = oe

  emails = sorted(set(cust_to_owner.keys()))
  if only_emails:
    emails = [e for e in emails if e in set(x.strip().lower() for x in only_emails)]
  results["emails_found"] = len(emails)
  if not emails:
    return results

  frm = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
  to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
  cutoff = datetime.now(timezone.utc) - timedelta(days=days)

  contacts_by_owner: Dict[str, List[str]] = defaultdict(list)
  for ce in emails:
    contacts_by_owner[cust_to_owner[ce]].append(ce)

  for organizer_email, customers in contacts_by_owner.items():
    all_items = av_list_meetings_by_organizer(avoma_api_key, organizer_email, frm, to, limit, pages, delay)
    if not all_items:
      continue

    for customer_email in customers:
      filtered: List[Dict[str, Any]] = []
      for m in all_items:
        atts = attendees_emails(m)
        org = (m.get("organizer_email") or "").strip().lower()
        owner_in_attendees = organizer_email in atts

        if not (org == organizer_email or owner_in_attendees):
          continue
        if customer_email not in atts:
          continue
        if not looks_like_call(m):
          continue
        dt = parse_iso(m.get("start_at") or m.get("startTime") or m.get("start_time") or m.get("start"))
        if dt and dt < cutoff:
          continue

        filtered.append(m)

      if not filtered:
        continue

      filtered.sort(key=lambda mm: parse_iso(mm.get("start_at") or mm.get("startTime") or mm.get("start_time") or mm.get("start")) or datetime.max.replace(tzinfo=timezone.utc))
      m = filtered[0]
      meeting_id = m.get("uuid") or m.get("id") or m.get("meetingId")
      if not meeting_id:
        continue

      t_json = av_fetch_transcript_json(avoma_api_key, meeting_id, m.get("transcription_uuid"))
      if not t_json:
        continue
      plain = transcript_json_to_text(t_json)
      if not plain:
        continue

      payload = {
        "email": customer_email,
        "organizer_email": organizer_email,
        "meeting_id": meeting_id,
        "meeting_subject": m.get("subject"),
        "meeting_url": m.get("url") or m.get("app_url"),
        "start_at": m.get("start_at") or m.get("startTime") or m.get("start_time"),
        "attendees_emails": attendees_emails(m),
        "transcript_text": plain,
        "meeting_data": m,
        "source": "hs_list_avoma_plaintext",
        "processed_at": datetime.now(timezone.utc).isoformat()
      }

      wr = http_request("POST", webhook_url, headers={"Content-Type": "application/json"}, json=payload)
      if wr and wr.status_code in (200, 201, 202, 204):
        results["transcripts_posted"] += 1
      else:
        results["errors"].append(f"Webhook failed for meeting {meeting_id}: {(wr.status_code if wr else 'no-response')}")

      time.sleep(delay)
      results["meetings_found"] += len(filtered)

  return results

def parse_args() -> argparse.Namespace:
  p = argparse.ArgumentParser(description="HubSpot list -> Avoma transcript (plain text) -> Webhook")
  p.add_argument("--only-email", action="append", default=[], help="Process only this email (repeatable)")
  p.add_argument("--days", type=int, default=int(os.environ.get("DATE_FILTER_DAYS", "365")))
  p.add_argument("--limit", type=int, default=int(os.environ.get("AVOMA_LIMIT", "100")))
  p.add_argument("--pages", type=int, default=int(os.environ.get("AVOMA_PAGES", "5")))
  p.add_argument("--delay", type=float, default=float(os.environ.get("RATE_LIMIT_DELAY", "0.5")))
  return p.parse_args()

def require_env(keys: List[str]) -> Dict[str, str]:
  missing = [k for k in keys if not os.environ.get(k)]
  if missing:
    raise RuntimeError("Missing env: " + ", ".join(missing))
  return {k: os.environ[k] for k in keys}

def main() -> None:
  args = parse_args()
  env = require_env(["HUBSPOT_TOKEN", "HUBSPOT_LIST_ID", "AVOMA_API_KEY", "WEBHOOK_URL"])

  res = run_pipeline(
    hubspot_token=env["HUBSPOT_TOKEN"],
    hubspot_list_id=env["HUBSPOT_LIST_ID"],
    avoma_api_key=env["AVOMA_API_KEY"],
    webhook_url=env["WEBHOOK_URL"],
    only_emails=args.only_email or None,
    days=args.days,
    limit=args.limit,
    pages=args.pages,
    delay=args.delay,
  )

  print("\n" + "="*48)
  print("SUMMARY")
  print("="*48)
  print(json.dumps(res, indent=2))

if __name__ == "__main__":
  main()
