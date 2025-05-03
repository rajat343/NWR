#!/usr/bin/env python3
import time
import random
import grpc
import tasks_pb2, tasks_pb2_grpc

def main():
    stubs = []
    for i in range(5):
        ch = grpc.insecure_channel(f"localhost:{50050+i}")
        stubs.append(tasks_pb2_grpc.TaskServiceStub(ch))

    for idx in range(20):
        name = f"ClientTask_{idx}"
        weight = random.randint(1, 100)
        target = random.choice(stubs)
        res = target.SendTask(tasks_pb2.TaskRequest(name=name, weight=weight))
        print(f"Sent {name} w={weight}, ack={res.success}")
        time.sleep(0.5)

    primary = stubs[0]
    wr = primary.WriteData(tasks_pb2.WriteRequest(key="k1", value="v1"))
    print("WriteData ack=", wr.success)
    rd = primary.ReadData(tasks_pb2.ReadRequest(key="k1", read_quorum=2))
    print(f"ReadData v={rd.value}, served_by={rd.served_by}")

if __name__ == '__main__':
    main()