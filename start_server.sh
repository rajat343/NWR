#!/usr/bin/env bash
VENV_PATH="$(pwd)/venv"
MAX=100
PORTS=(50050 50051 50052 50053 50054)
for i in "${!PORTS[@]}"; do
  PORT=${PORTS[$i]}
  osascript <<EOF
 tell application "Terminal"
   activate
   do script "cd $(pwd) && source \"$VENV_PATH/bin/activate\" && python3 server.py --id $i --port $PORT --max_tasks $MAX"
 end tell
EOF
 done