#!/usr/bin/env python3
import time, random, grpc, threading
import tasks_pb2, tasks_pb2_grpc

PORT_RANGE = range(50050, 50060)  # Support up to 10 servers
EXCLUDED_SERVERS = {2}  # Optional: initially avoid server 2

# Shared list of active stubs
active_stubs = {}
lock = threading.Lock()

SERVER_IPS = {
    0: "192.168.1.2",
    1: "192.168.1.2",
    2: "192.168.1.1",
    3: "192.168.1.1",
    4: "192.168.1.1",
}

def discover_servers():
    while True:
        for port in PORT_RANGE:
            server_id = port - 50050
            if server_id in active_stubs:
                continue
            try:
                ch = grpc.insecure_channel(f"{SERVER_IPS[server_id]}:{port}")
                grpc.channel_ready_future(ch).result(timeout=1)
                stub = tasks_pb2_grpc.TaskServiceStub(ch)
                with lock:
                    active_stubs[server_id] = stub
                print(f"Connected to server {server_id} on port {port}")
            except:
                continue
        time.sleep(2)

def rpc_with_failover(call_fn, retries=3):
    for _ in range(retries):
        with lock:
            eligible = {k: v for k, v in active_stubs.items() if k not in EXCLUDED_SERVERS}
        items = list(eligible.items())
        if not items:
            time.sleep(1)
            continue
        random.shuffle(items)
        for server_id, stub in items:
            try:
                return call_fn(stub), server_id
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    continue
                raise
    raise RuntimeError("All servers unavailable")

def main():
    threading.Thread(target=discover_servers, daemon=True).start()
    time.sleep(3)  # Allow some time to populate stubs

    for idx in range(500):
        name = f"ClientTask_{idx}"
        weight = random.randint(1, 100)
        try:
            res, sid = rpc_with_failover(
                lambda stub: stub.SendTask(tasks_pb2.TaskRequest(
                    name=name, weight=weight, replicated=False, source_id=-1
                ))
            )
            print(f"Client → Sent {name} w={weight} to server {sid}, ack={res.success}")
        except Exception as e:
            print(f"Failed to send {name}: {e}")
        time.sleep(0.5)

    # Optional demo: NWR write/read
    with lock:
        if 0 in active_stubs:
            primary = active_stubs[0]
            wr = primary.WriteData(tasks_pb2.WriteRequest(key="k1", value="v1"))
            print("Client → WriteData ack =", wr.success)
            rd = primary.ReadData(tasks_pb2.ReadRequest(key="k1", read_quorum=2))
            print(f"Client → ReadData v={rd.value}, served_by={rd.served_by}")

if __name__ == '__main__':
    main()
