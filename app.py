from flask import Flask, render_template, request, jsonify
import os

from DAL.whitelist_db import WhitelistDB, DB_PATH

app = Flask(__name__)

db = WhitelistDB()


def check_hostname_in_whitelist(host_query: str):
    """
    Business logic only:
    - Normalization and validation
    - Deciding between exact vs wildcard
    - Building the response dict
    """

    host_query = host_query.strip().lower()

    if not host_query:
        return {
            "allowed": False,
            "mode": "invalid",
            "match_count": 0,
            "matches": [],
            "reason": "Empty hostname",
        }

    allowed_chars = set("abcdefghijklmnopqrstuvwxyz0123456789.-*")
    if any(c not in allowed_chars for c in host_query):
        return {
            "allowed": False,
            "mode": "invalid",
            "match_count": 0,
            "matches": [],
            "reason": "Hostname has invalid characters",
        }

    # Wildcard mode like *.example.com
    if host_query.startswith("*.") and len(host_query) > 2:
        domain = host_query[2:]

        matches = db.suffix_matches(domain, limit=20)
        match_count = len(matches)

        return {
            "allowed": match_count > 0,
            "mode": "wildcard",
            "match_count": match_count,
            "matches": matches,
            "reason": "Found hosts with that suffix" if matches else "No hosts match this wildcard",
        }

    # Exact match mode
    matches = db.exact_matches(host_query)
    match_count = len(matches)

    return {
        "allowed": match_count > 0,
        "mode": "exact",
        "match_count": match_count,
        "matches": matches,
        "reason": "Exact hostname found in whitelist" if matches else "Hostname not present in whitelist",
    }


@app.route("/", methods=["GET", "POST"])
def index():
    result = None
    hostname = ""

    if request.method == "POST":
        hostname = request.form.get("hostname", "")
        result = check_hostname_in_whitelist(hostname)

    return render_template("index.html", hostname=hostname, result=result)


@app.route("/api/check", methods=["GET", "POST"])
def api_check():
    """
    Simple JSON API for hostname checks.

    Usage examples:
      - GET  /api/check?hostname=example.com
      - POST /api/check with form-data or JSON: {"hostname": "example.com"}
    """
    hostname = ""

    if request.method == "GET":
        hostname = request.args.get("hostname", "")
    else:  # POST
        if request.is_json:
            data = request.get_json(silent=True) or {}
            hostname = data.get("hostname", "")
        else:
            hostname = request.form.get("hostname", "")

    result = check_hostname_in_whitelist(hostname)
    # You could wrap with extra fields if you like, e.g. {"hostname": hostname, "result": result}
    return jsonify(result)


if __name__ == "__main__":
    # Simple guard to make sure DB exists
    if not os.path.exists(DB_PATH):
        print("Database file does not exist yet.")
        print("Run `python load_hostnames.py` first to create and populate it.")
    app.run(debug=True)

