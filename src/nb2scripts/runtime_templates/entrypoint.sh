#!/bin/sh
set -e
if [ -n "$SCRIPT" ]; then
  exec python /app/scripts/$SCRIPT
else
  exec "$@"
fi