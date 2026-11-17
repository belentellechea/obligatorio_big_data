#!/usr/bin/env bash
# Empaqueta el código para spark-submit en el cluster (--py-files datalake.zip)
set -euo pipefail
cd "$(dirname "$0")/src" && zip -qr ../datalake.zip datalake -x '*__pycache__*'
echo "datalake.zip listo"
