# shared/bing_search.py
import os, json, itertools, requests
from concurrent.futures import ThreadPoolExecutor, as_completed

SERPER_ENDPOINT = "https://google.serper.dev/search"
SERPER_KEY      = os.getenv("SERPER_API_KEY")          # export SERPER_API_KEY=...
HEADERS         = {"X-API-KEY": SERPER_KEY,
                   "Content-Type": "application/json"}

def _call_serper(query: str, num: int = 5) -> dict:
    """Low-level request to Serper; returns {} on error."""
    payload = json.dumps({"q": query, "num": num})
    try:
        resp = requests.post(SERPER_ENDPOINT, headers=HEADERS, data=payload, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        print(f"[Serper] {resp.status_code} – {resp.text[:120]}")
    except requests.RequestException as exc:
        print(f"[Serper] request failed: {exc}")
    return {}

def _grouper(iterable, n):
    "Collect data into fixed-length chunks (last chunk may be shorter)."
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, n))
        if not batch:
            break
        yield batch

def single_bing_search(keyphrases: list[str],
                       batch_size: int = 5,
                       max_workers: int = 8) -> dict:
    """
    Executes Serper searches concurrently.
    - keyphrases: list of search strings
    - returns: combined Serper JSON (aggregated 'organic' hits etc.)
    """
    # 1️⃣  create one OR-joined query per batch
    queries = [" OR ".join(batch)[:2048]          # truncate defensively
               for batch in _grouper(keyphrases, batch_size)]

    # 2️⃣  launch them in parallel
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_q = {pool.submit(_call_serper, q): q for q in queries}
        for fut in as_completed(future_to_q):
            results.append(fut.result())

    # 3️⃣  merge – here we keep the structure simple: copy the first JSON,
    #      then extend the 'organic' list with the rest.
    if not results:
        return {}

    combined = results[0] or {}
    for res in results[1:]:
        if not res:
            continue
        # merge 'organic' lists
        combined.setdefault("organic", []).extend(res.get("organic", []))
        # you can merge other keys similarly if you rely on them

    return combined
