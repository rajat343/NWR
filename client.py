#!/usr/bin/env python3
import time
import random
import grpc
import tasks_pb2
import tasks_pb2_grpc

def rpc_with_failover(call_fn, stubs, retries=4, **kw):
    """
    Try the RPC on a random stub; on StatusCode.UNAVAILABLE
    shuffle to another. Retries on gRPC failure or application-level ack=False.
    Raises RuntimeError only if *all* stubs are down or reject within <retries> passes.
    """
    for attempt in range(retries):
        random.shuffle(stubs)
        for stub in stubs:
            try:
                res = call_fn(stub, **kw)
                if hasattr(res, 'success') and not res.success:
                    print("→ RPC failed (ack=False), retrying...")
                    continue
                return res
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("→ gRPC UNAVAILABLE – trying next peer...")
                    continue
                print(f"→ gRPC Exception: {e}")
                raise
    raise RuntimeError("❌ All stubs unavailable or returned ack=False")

def main():
    print("⚙️ Initializing gRPC stubs...")
    stubs = []
    for i in range(5):
        ch = grpc.insecure_channel(f"localhost:{50050+i}")
        try:
            grpc.channel_ready_future(ch).result(timeout=2)
        except Exception as e:
            print(f"⚠️ Server {i} not ready: {e}")
        stubs.append(tasks_pb2_grpc.TaskServiceStub(ch))

    # ❗️Exclude server 2 from initial task load
    eligible_stubs = [stub for i, stub in enumerate(stubs) if i != 2]

    print("🚀 Sending tasks...")
    for idx in range(50):
        name = f"ClientTask_{idx}"
        weight = random.randint(1, 100)

        try:
            res = rpc_with_failover(
                lambda s: s.SendTask(tasks_pb2.TaskRequest(
                    name=name,
                    weight=weight,
                    replicated=False,
                    source_id=-1
                )),
                stubs=eligible_stubs
            )
            print(f"✅ Sent {name} w={weight}")
        except Exception as e:
            print(f"❌ FAILED to send {name} w={weight}: {e}")

        time.sleep(0.2)  # Reduced sleep for faster testing

    print("⏳ Waiting for task propagation...")
    time.sleep(5)

    print("📡 Testing NWR Write/Read...")
    primary = stubs[0]
    try:
        wr = rpc_with_failover(
            lambda s: s.WriteData(tasks_pb2.WriteRequest(key="k1", value="v1")),
            stubs
        )
        print("✅ WriteData ack =", wr.success)

        rd = rpc_with_failover(
            lambda s: s.ReadData(tasks_pb2.ReadRequest(key="k1", read_quorum=2)),
            stubs
        )
        print(f"✅ ReadData value = {rd.value}, served_by = {rd.served_by}")
    except Exception as e:
        print(f"❌ Read/Write failed: {e}")

    print("✅ Client finished.")

if __name__ == '__main__':
    main()
