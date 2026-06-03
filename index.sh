#!/bin/bash

echo "=========================================="
echo "Bluesky Inverted Index"
echo "=========================================="

echo "Building Index..."
python3 build_index.py

echo "Inverted Index successfully built!"
read -p "Press any key to continue..." -n1 -s
echo ""