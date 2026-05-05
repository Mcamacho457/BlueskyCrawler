#!/bin/bash

echo "=========================================="
echo "Bluesky Post Collector"
echo "=========================================="

cd "$(dirname "$0")"

if ! command -v python3 &> /dev/null
then
    echo "ERROR: Python3 is not installed."
    echo "Please install Python3 and try again."
    exit 1
fi

echo "Installing required package: atproto, requests, & beautifulsoup4"
python3 -m pip install --user atproto requests beautifulsoup4

echo ""
echo "Running crawlers in required order..."
echo ""

echo "Step 1: Running Conspiracy Theories crawler first..."
python3 conspiracy_theories_crawl.py "$1" "$2" "$3" "$4"

echo ""
echo "Step 2: Paranormal Activity  crawler..."
python3 paranormal_theories_crawl.py

echo ""
echo "Step 3: Running UFO Theories crawler..."
python3 ufo_theories_crawl.py

echo ""
echo "Step 4: Running Strange Earth crawler..."
python3 Strange_Earth_crawl.py

echo ""
echo "=========================================="
echo "Collection complete."
echo "All crawlers finished."
echo "=========================================="