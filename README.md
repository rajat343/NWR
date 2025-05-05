```bash
python -m venv venv
source venv/bin/activate
```

```bash
pip install -r requirements.txt
```

```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. tasks.proto
```

```bash
sh start_server.sh
```

```bash
sh start_client.sh
```

```bash
sh add_server.sh 5 50055 '{"0":1,"1":2,"2":3,"3":4,"4":5}'
```
