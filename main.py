import os
from flask import Flask, jsonify

app = Flask(name)

@app.route("/")
def healthcheck():
    return jsonify({"status": "ok", "service": "Aletheia DataCore"})

if name == "main":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
