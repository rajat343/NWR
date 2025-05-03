```bash
python -m venv venv
source venv/bin/activate
```

```
pip install -r requirements.txt
```

```
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. tasks.proto
```
