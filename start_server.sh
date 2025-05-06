#!/usr/bin/env bash
VENV="$(pwd)/venv"
MAX=100
PROJECT_DIR="$(pwd)"

D0='{"1":1,"2":2,"3":3,"4":4}'
D1='{"0":1,"2":1,"3":2,"4":3}'
D2='{"0":2,"1":1,"3":1,"4":2}'
D3='{"0":3,"1":2,"2":1,"4":1}'
D4='{"0":4,"1":3,"2":2,"3":1}'

for i in 0 1; do 
  #Here in one system we are starting two servers, in other system we are starting 2,3,4
  PORT=$((50050 + i))
  DIST_VAR="D$i"
  DISTANCES="${!DIST_VAR}"
  DIST_ESCAPED=${DISTANCES//\"/\\\"}

  osascript <<EOF
tell application "Terminal"
  activate
  do script "cd '$PROJECT_DIR'; source '$VENV/bin/activate'; python3 server.py --id $i --port $PORT --max_tasks $MAX --distances \"$DIST_ESCAPED\""
end tell
EOF

  sleep 0.3
done
