#!/bin/bash

for i in {0..4}
do
  port=$((50050 + i))

  case $i in
    0) dist='{\"1\":1,\"2\":2,\"3\":3,\"4\":4}' ;;
    1) dist='{\"0\":1,\"2\":1,\"3\":2,\"4\":3}' ;;
    2) dist='{\"0\":2,\"1\":1,\"3\":1,\"4\":2}' ;;
    3) dist='{\"0\":3,\"1\":2,\"2\":1,\"4\":1}' ;;
    4) dist='{\"0\":4,\"1\":3,\"2\":2,\"3\":1}' ;;
  esac

  osascript <<EOF
tell application "Terminal"
  do script "cd \"$(pwd)\"; source venv/bin/activate; python3 server.py --id $i --port $port --dist '$dist'"
end tell
EOF

  sleep 1
done