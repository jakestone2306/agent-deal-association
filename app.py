import os
import threading
import traceback
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

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "agent": "deal-association-agent"})

@app.route("/status", methods=["GET"])
def status():
    return jsonify(run_state)

@app.route("/backfill", methods=["POST"])
def backfill():
    """Backfill all deals missing associations."""
    if run_state["status"] == "running":
        return jsonify({"status": "already_running"}), 409
    t = threading.Thread(target=run_backfill_bg)
    t.daemon = True
    t.start()
    return jsonify({"status": "started", "message": "Backfill running in background. Check /status for updates."})

@app.route("/associate", methods=["POST"])
def associate_single():
    """Associate a single deal. Called via HubSpot webhook on deal creation.
    Expects JSON body: {"deal_id": "123", "deal_name": "Acme Corp"}
    OR query param: ?deal_id=123
    """
    data = request.get_json(silent=True) or {}
    deal_id   = data.get("deal_id") or request.args.get("deal_id")
    deal_name = data.get("deal_name","")

    if not deal_id:
        return jsonify({"error": "Missing deal_id"}), 400

    # If no name passed, fetch it from HubSpot
    if not deal_name:
        import requests as req
        r = req.get(
            f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}",
            headers={"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}"},
            params={"properties": "dealname"},
        )
        if r.ok:
            deal_name = r.json().get("properties", {}).get("dealname", "")

    result = associate_deal(deal_id, deal_name)
    return jsonify({"status": "success", "result": result})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
