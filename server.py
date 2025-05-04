#!/usr/bin/env python3
import os
import time
import threading
import queue
import psutil
import grpc
from concurrent import futures
import tasks_pb2, tasks_pb2_grpc
from multiprocessing import Value

# Distance matrix (latencies)
DISTANCES = {
    0: {1:1, 2:2, 3:3, 4:4},
    1: {0:1, 2:1, 3:2, 4:3},
    2: {0:2, 1:1, 3:1, 4:2},
    3: {0:3, 1:2, 2:1, 4:1},
    4: {0:4, 1:3, 2:2, 3:1},
}

# NWR parameters
N = 5     # total nodes
W = 3     # write quorum
R = 2     # read quorum

class NWRServer(tasks_pb2_grpc.TaskServiceServicer):
    def __init__(self, server_id, port, global_counter, max_tasks):
        self.id = server_id
        self.port = port
        self.global_counter = global_counter
        self.max_tasks = max_tasks
        self.task_queue = queue.Queue()
        self.replication_queue = queue.Queue()
        self.data_store = {}
        self.peers = {}
        self.lock = threading.Lock()

        print(f"[{self.id}] Server initialized with empty task queue.")
        # start background loops
        self.start_replication_loop()

    # --- Task RPCs ---
    def SendTask(self, req, ctx):
        with self.lock:
            self.task_queue.put((req.name, req.weight))
            print(f"[{self.id}] Received task {req.name} (w={req.weight}) from client/peer")
        return tasks_pb2.Ack(success=True)

    def GetQueueLength(self, req, ctx):
        return tasks_pb2.QueueLengthResponse(length=self.task_queue.qsize())

    def GetCPUUsage(self, req, ctx):
        cpu = psutil.cpu_percent(interval=0.1)
        return tasks_pb2.CPUUsageResponse(usage=cpu)

    def StealTask(self, req, ctx):
        with self.lock:
            if not self.task_queue.empty():
                name, w = self.task_queue.get()
                print(f"[{self.id}] Providing stolen task {name} (w={w})")
                return tasks_pb2.TaskResponse(
                    task=tasks_pb2.TaskRequest(name=name, weight=w), success=True
                )
        return tasks_pb2.TaskResponse(success=False)

    def Heartbeat(self, req, ctx):
        return tasks_pb2.HeartbeatResponse(timestamp=int(time.time()))

    # --- NWR Replication RPCs ---
    def WriteData(self, req, ctx):
        self.data_store[req.key] = req.value
        ack_count = 1
        for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1])[:W-1]:
            stub = self.peers.get(peer_id)
            if stub:
                try:
                    res = stub.WriteData(req)
                    if res.success:
                        ack_count += 1
                except:
                    pass
        success = ack_count >= W
        print(f"[{self.id}] WriteData key={req.key}, success={success} (acks={ack_count})")
        return tasks_pb2.Ack(success=success)

    def ReadData(self, req, ctx):
        if req.key in self.data_store:
            return tasks_pb2.ReadResponse(
                value=self.data_store[req.key], success=True, served_by=self.id
            )
        for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1]):
            stub = self.peers.get(peer_id)
            if stub:
                try:
                    resp = stub.ReadData(req)
                    if resp.success:
                        return tasks_pb2.ReadResponse(
                            value=resp.value, success=True, served_by=peer_id
                        )
                except:
                    pass
        return tasks_pb2.ReadResponse(success=False)

    # --- Background Loops ---
    def start_replication_loop(self):
        def rep_loop():
            while True:
                name, w = self.replication_queue.get()
                for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1])[:W]:
                    stub = self.peers.get(peer_id)
                    if stub:
                        try:
                            stub.SendTask(tasks_pb2.TaskRequest(name=name, weight=w))
                            print(f"[{self.id}] Replicated task {name} to server {peer_id}")
                        except Exception as e:
                            print(f"[{self.id}] Error replicating to {peer_id}: {e}")
                self.replication_queue.task_done()
        threading.Thread(target=rep_loop, daemon=True).start()

    def start_processing(self):
        def loop():
            while True:
                if not self.task_queue.empty():
                    name, w = self.task_queue.get()
                    start = time.time()
                    for _ in range(w * 1_000_000):
                        pass
                    elapsed = time.time() - start
                    print(f"[{self.id}] Executed {name} w={w} in {elapsed:.2f}s")

                    # enqueue for replication
                    self.replication_queue.put((name, w))

                    with self.global_counter.get_lock():
                        self.global_counter.value += 1
                        if self.global_counter.value >= self.max_tasks:
                            print(f"[{self.id}] Max tasks reached. Exiting.")
                            os._exit(0)

                    # attempt stealing
                    self.try_steal()
                time.sleep(0.5)
        threading.Thread(target=loop, daemon=True).start()

    def try_steal(self):
        my_len = self.task_queue.qsize()
        my_cpu = psutil.cpu_percent(interval=0.1)
        for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1]):
            stub = self.peers.get(peer_id)
            if not stub:
                continue
            try:
                other_len = stub.GetQueueLength(tasks_pb2.Empty()).length
                other_cpu = stub.GetCPUUsage(tasks_pb2.Empty()).usage
                if other_len > my_len + 1 and other_cpu < my_cpu:
                    resp = stub.StealTask(tasks_pb2.Empty())
                    if resp.success:
                        self.task_queue.put((resp.task.name, resp.task.weight))
                        print(f"[{self.id}] Stealing task {resp.task.name} from server {peer_id}")
                        break
            except Exception as e:
                print(f"[{self.id}] Error stealing from {peer_id}: {e}")
                continue

# --- Server Startup ---
def serve(id, port, global_counter, max_tasks):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = NWRServer(id, port, global_counter, max_tasks)
    tasks_pb2_grpc.add_TaskServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Server {id} listening on {port}")
    for pid in range(N):
        if pid == id: continue
        ch = grpc.insecure_channel(f"localhost:{50050+pid}")
        servicer.peers[pid] = tasks_pb2_grpc.TaskServiceStub(ch)
    servicer.start_processing()
    server.wait_for_termination()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--max_tasks', type=int, default=100)
    args = parser.parse_args()
    gc = Value('i', 0)
    serve(args.id, args.port, gc, args.max_tasks)