from flask import Flask, request, jsonify
from flask_cors import CORS
import csv, os

SAVE_DIR = "QualtricsTracker/logs"   
os.makedirs(SAVE_DIR, exist_ok=True)
HEADER = ["timestamp_iso","url","question_id","source","search_results"]

app = Flask(__name__)
CORS(app)

@app.get("/")
def index():
    return "URL Tracker collector is running. POST JSON to /ingest", 200

@app.route("/ingest", methods=["POST", "OPTIONS"])
@app.route("/ingest/", methods=["POST", "OPTIONS"])
def ingest():
    if request.method == "OPTIONS":
        return ("", 204)

    data = request.get_json(silent=True) or {}
    events = data.get("events", []) or []
    meta = data.get("meta") or {}
    overwrite = bool(
        data.get("overwrite")
        or meta.get("overwrite")
        or data.get("finalized")
        or meta.get("finalized")
    )
    response_hint = data.get("responseId") or meta.get("responseId")
    removed_count = data.get("removedCount")
    if removed_count is None:
        removed_count = meta.get("removedCount")

    print(f"[collector] received {len(events)} events", flush=True)

    events_by_id = {}
    for ev in events:
        rid = ev.get("responseId") or response_hint
        if not rid:
            continue
        events_by_id.setdefault(rid, []).append(ev)

    if overwrite and response_hint and response_hint not in events_by_id:
        events_by_id[response_hint] = []

    total_written = 0

    for rid, items in events_by_id.items():
        path = os.path.join(SAVE_DIR, f"{rid}.csv")
        mode = "w" if overwrite else "a"
        is_new = overwrite or not os.path.exists(path)

        with open(path, mode, newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow(HEADER)

            for ev in items:
                search_results = ev.get("searchResults")
                if isinstance(search_results, list):
                    search_str = " | ".join(str(item) for item in search_results)
                elif search_results is None:
                    search_str = ""
                else:
                    search_str = str(search_results)

                w.writerow([
                    ev.get("ts"),
                    ev.get("url"),
                    ev.get("questionId"),
                    ev.get("source"),
                    search_str,
                ])
                total_written += 1

        action = "overwrite" if overwrite else "append"
        print(f"[collector] {action} {len(items)} rows for {rid}", flush=True)

    if overwrite and not events_by_id and response_hint:
        path = os.path.join(SAVE_DIR, f"{response_hint}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)
        print(f"[collector] overwrite 0 rows for {response_hint}", flush=True)

    if overwrite and removed_count is not None:
        print(f"[collector] participant removed {removed_count} entries before sync", flush=True)

    print(f"[collector] total rows processed: {total_written}", flush=True)
    return jsonify(ok=True, saved=len(events))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
