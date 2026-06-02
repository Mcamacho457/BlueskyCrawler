import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from flask import Flask, render_template, request
from search_engine import search

app = Flask(__name__)
INDEX_DIR = "../bluesky_index"

def search_posts(query, top_k):
    if query.strip():
        results = search(INDEX_DIR, query, top_k=top_k)
        return results
    return []


@app.route("/", methods=["GET", "POST"])
def index():
    results = []
    query = ""
    top_k = 0

    if request.method == "POST":
        query = request.form.get("query", "")
        top_k = int(request.form.get("topk", ""))
        results = search_posts(query, top_k)

    return render_template("index.html", query=query, results=results, topk=top_k)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
