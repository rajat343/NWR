#!/usr/bin/env python3
import grpc, random, time
import tasks_pb2, tasks_pb2_grpc

def rpc_with_failover(call_fn, stubs, retries=4, **kw):
    for _ in range(retries):
        random.shuffle(stubs)
        for stub in stubs:
            try:
                return call_fn(stub, **kw)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    continue
                raise
    raise RuntimeError("All stubs unavailable")

def main():
    stubs = []
    for i in range(6):  # search up to port 50055
        try:
            ch = grpc.insecure_channel(f"localhost:{50050 + i}")
            grpc.channel_ready_future(ch).result(timeout=1)
            stubs.append(tasks_pb2_grpc.TaskServiceStub(ch))
        except:
            continue

    for idx in range(100):
        name = f"ClientTask_{idx}"
        weight = random.randint(1, 100)
        eligible_stubs = [s for i, s in enumerate(stubs) if i != 2]
        rpc_with_failover(
            lambda s: s.SendTask(tasks_pb2.TaskRequest(
                name=name, weight=weight,
                replicated=False, source_id=-1
            )),
            stubs=eligible_stubs
        )
        print(f"Client → Sent {name} w={weight}")
        time.sleep(0.3)

if __name__ == "__main__":
    main()
