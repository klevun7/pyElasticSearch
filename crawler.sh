#!/bin/bash
# Usage: ./crawler.sh seed.txt 10000 output/

# Activate venv
source venv/bin/activate

# Run crawler
python3 crawler.py "$1" "$2" "$3"