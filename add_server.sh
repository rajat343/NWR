# add_server.sh
#!/usr/bin/env bash
VENV="$(pwd)/venv"
MAX=100

if [ $# -ne 3 ]; then
  echo "Usage: $0 <new_id> <new_port> '<dist_json>'"
  echo " e.g.: $0 5 50055 '{\"0\":1,\"1\":2,\"2\":3,\"3\":4,\"4\":5}'"
  exit 1
fi

NEW_ID=$1
NEW_PORT=$2
DIST_JSON=$3
DIST_ESCAPED=${DIST_JSON//\"/\\\"}

osascript <<EOF
tell application "Terminal"
  activate
  do script "cd '$(pwd)'; source '$VENV/bin/activate'; python3 server.py --id $NEW_ID --port $NEW_PORT --max_tasks $MAX --distances \"$DIST_ESCAPED\""
end tell
EOF
