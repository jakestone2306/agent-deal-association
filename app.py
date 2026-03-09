import os
import threading
import traceback
import requests as req
from flask import Flask, request, jsonify
from agent import associate_deal, backfill_all

app = Flask(__name__)

run_state = {"status": "idle", "last_run": None, "last_result": None, "last_error": None}

def run_backfill_bg():
    from datetime import datetime
    run_state["status"] = "running"
    run_state["last_run"] = datetime.utcnow().isoformat()
    try:
        result = backfill_all()
        run_state["status"] = "success"
        run_state["last_result"] = result
        run_state["last_error"] = None
    except Exception as e:
        run_state["status"] = "error"
        run_state["last_error"] = traceback.format_exc()

def fetch_deal_name(deal_id):
    """Fetch deal name from HubSpot by ID."""
    try:
        r = req.get(
            f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}",
            headers={"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}"},
            params={"properties": "dealname"},
        )
        if r.ok:
            return r.json().get("properties", {}).get("dealname", "")
    except:
        pass
    return ""

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "agent": "deal-association-agent", "run_state": run_state})

@app.route("/status", methods=["GET"])
def status():
    return jsonify(run_state)

@app.route("/backfill", methods=["POST"])
def backfill():
    if run_state["status"] == "running":
        return jsonify({"status": "already_running"}), 409
    t = threading.Thread(target=run_backfill_bg)
    t.daemon = True
    t.start()
    return jsonify({"status": "started", "message": "Backfill running in background. Check /status."})

@app.route("/associate", methods=["POST"])
def associate_single():
    """
    Handles two formats:
    1. HubSpot webhook: [{"subscriptionType": "deal.creation", "objectId": 123}]
    2. Direct call:     {"deal_id": "123", "deal_name": "Acme Corp"}
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "No JSON body"}), 400

    # Handle HubSpot webhook format (array of events)
    if isinstance(data, list):
        results = []
        for event in data:
            deal_id   = str(event.get("objectId", ""))
            deal_name = fetch_deal_name(deal_id) if deal_id else ""
            if deal_id:
                result = associate_deal(deal_id, deal_name)
                results.append(result)
        return jsonify({"status": "success", "results": results})

    # Handle direct call format
    deal_id   = str(data.get("deal_id") or request.args.get("deal_id", ""))
    deal_name = data.get("deal_name", "") or fetch_deal_name(deal_id)

    if not deal_id:
        return jsonify({"error": "Missing deal_id"}), 400

    result = associate_deal(deal_id, deal_name)
    return jsonify({"status": "success", "result": result})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
