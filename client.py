#!/usr/bin/env python3
import time, random, grpc
import tasks_pb2, tasks_pb2_grpc


# ──────────────────────────────────────────────────────────────
# Fault tolerance helper function for client RPC calls
# ──────────────────────────────────────────────────────────────
def rpc_with_failover(call_fn, stubs, retries=4, **kw):
    """
    Try the RPC on a random stub; on StatusCode.UNAVAILABLE
    shuffle to another.  Raises RuntimeError only if *all*
    stubs are down for <retries> full passes.
    """
    for _ in range(retries):
        random.shuffle(stubs)
        for stub in stubs:
            try:
                return call_fn(stub, **kw)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    continue          # that peer is down – try next
                raise                 # any other gRPC error = real bug
    raise RuntimeError("all stubs unavailable")


def main():
    # Create stubs for all 5 servers
    stubs = [tasks_pb2_grpc.TaskServiceStub(
                grpc.insecure_channel(f"localhost:{50050+i}"))
             for i in range(5)]

    # Send 20 tasks using the failover mechanism
    for idx in range(20):
        name = f"ClientTask_{idx}"
        weight = random.randint(1, 100)

        # Use the failover helper for sending tasks
        rpc_with_failover(
            lambda s: s.SendTask(tasks_pb2.TaskRequest(
                name=name, weight=weight,
                replicated=False, source_id=-1
            )),
            stubs=stubs
        )

        print(f"Client → Sent {name} w={weight}")
        time.sleep(0.5)

    # Optional pause so some tasks finish
    time.sleep(2)

    # Use the failover helper for fetching a result
    target_name = f"ClientTask_{random.randint(0,19)}"
    resp = rpc_with_failover(
        lambda s: s.GetResult(
            tasks_pb2.GetResultRequest(name=target_name),
            timeout=1.0
        ),
        stubs=stubs
    )

    if resp.success:
        print(f"Client ← RESULT {target_name}  {resp.result}  (served by {resp.served_by})")
    else:
        print(f"Client ← {target_name} not found yet")

    # Demonstrate N-W-R operations with failover
    primary = stubs[0]
    wr = primary.WriteData(tasks_pb2.WriteRequest(key="k1", value="v1"))
    print("Client → WriteData ack=", wr.success)
    rd = primary.ReadData(tasks_pb2.ReadRequest(key="k1", read_quorum=2))
    print(f"Client → ReadData v={rd.value}, served_by={rd.served_by}")


if __name__ == '__main__':
    main()