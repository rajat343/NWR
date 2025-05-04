#!/usr/bin/env python3
import os, time, threading, queue, psutil, grpc
from concurrent import futures
from multiprocessing import Value

import tasks_pb2, tasks_pb2_grpc

# Distance (latency) matrix for 5 servers
DISTANCES = {
    0: {1:1, 2:2, 3:3, 4:4},
    1: {0:1, 2:1, 3:2, 4:3},
    2: {0:2, 1:1, 3:1, 4:2},
    3: {0:3, 1:2, 2:1, 4:1},
    4: {0:4, 1:3, 2:2, 3:1},
}

# NWR parameters
N = 5    # total nodes
W = 3    # write quorum
R = 2    # read quorum

class NWRServer(tasks_pb2_grpc.TaskServiceServicer):
    def __init__(self, server_id, port, global_counter, max_tasks):
        self.id      = server_id
        self.port    = port
        self.max_tasks = max_tasks
        self.global_counter = global_counter

        # Separate queues:
        self.local_queue        = queue.Queue()  # client+stolen tasks (stealable)
        self.rep_received_queue = queue.Queue()  # incoming replication (non-stealable)
        self.replication_queue  = queue.Queue()  # tasks to send out

        self.data_store = {}
        self.peers      = {}
        self.lock       = threading.Lock()

        print(f"[{self.id}] Initialized with empty queues.")

        # start our two background loops
        self.start_replication_loop()
        self.start_processing()

    # --- RPC endpoints ---  
    def SendTask(self, req, ctx):
        """Handles both client submits (replicated=False) and peer replication."""
        if req.replicated:
            with self.lock:
                self.rep_received_queue.put((req.name, req.weight))
            print(f"[{self.id}] Received REPLICATED task {req.name} (w={req.weight}) from server {req.source_id}")
        else:
            with self.lock:
                self.local_queue.put((req.name, req.weight))
            print(f"[{self.id}] Received NEW task {req.name} (w={req.weight}) from client")
        return tasks_pb2.Ack(success=True)

    def GetQueueLength(self, req, ctx):
        return tasks_pb2.QueueLengthResponse(length=self.local_queue.qsize())

    def GetCPUUsage(self, req, ctx):
        usage = psutil.cpu_percent(interval=0.1)
        return tasks_pb2.CPUUsageResponse(usage=usage)

    def StealTask(self, req, ctx):
        with self.lock:
            if not self.local_queue.empty():
                name, w = self.local_queue.get()
                print(f"[{self.id}] Providing STOLEN task {name} (w={w})")
                # stolen tasks propagate as regular (replicated=False)
                return tasks_pb2.TaskResponse(
                    task=tasks_pb2.TaskRequest(name=name, weight=w, replicated=False, source_id=self.id),
                    success=True
                )
        return tasks_pb2.TaskResponse(success=False)

    def Heartbeat(self, req, ctx):
        return tasks_pb2.HeartbeatResponse(timestamp=int(time.time()))

    def WriteData(self, req, ctx):
        # NWR write: local + W−1 nearest peers
        self.data_store[req.key] = req.value
        acks = 1
        for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1])[:W-1]:
            stub = self.peers.get(peer_id)
            if not stub: continue
            try:
                res = stub.WriteData(req)
                if res.success: acks += 1
            except: pass
        success = (acks >= W)
        print(f"[{self.id}] WriteData key={req.key} → success={success} (acks={acks})")
        return tasks_pb2.Ack(success=success)

    def ReadData(self, req, ctx):
        if req.key in self.data_store:
            return tasks_pb2.ReadResponse(value=self.data_store[req.key], success=True, served_by=self.id)
        # else ask nearest peers up to R
        for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1]):
            stub = self.peers.get(peer_id)
            if not stub: continue
            try:
                resp = stub.ReadData(req)
                if resp.success:
                    return tasks_pb2.ReadResponse(value=resp.value, success=True, served_by=peer_id)
            except: pass
        return tasks_pb2.ReadResponse(success=False)

    # --- Background loops ---
    def start_replication_loop(self):
        def rep_loop():
            while True:
                name, w = self.replication_queue.get()
                for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1])[:W]:
                    stub = self.peers.get(peer_id)
                    if not stub: continue
                    try:
                        stub.SendTask(tasks_pb2.TaskRequest(
                            name=name, weight=w,
                            replicated=True, source_id=self.id
                        ))
                        print(f"[{self.id}] Replicated {name} → server {peer_id}")
                    except Exception as e:
                        print(f"[{self.id}] Replicate err→{peer_id}: {e}")
                self.replication_queue.task_done()
        threading.Thread(target=rep_loop, daemon=True).start()

    def start_processing(self):
        def proc_loop():
            while True:
                # choose local first (stealable), then replicated
                item = None
                if not self.local_queue.empty():
                    item = ('local',) + self.local_queue.get()
                elif not self.rep_received_queue.empty():
                    item = ('rep',) + self.rep_received_queue.get()
                else:
                    time.sleep(0.2)
                    continue

                src, name, w = item
                start = time.time()
                # simulate work
                for _ in range(w * 1_000_000): pass
                elapsed = time.time() - start
                print(f"[{self.id}] Executed {name} w={w} in {elapsed:.2f}s (src={src})")
                # only newly‐executed local tasks replicate
                if src == 'local':
                    self.replication_queue.put((name, w))

                with self.global_counter.get_lock():
                    self.global_counter.value += 1
                    if self.global_counter.value >= self.max_tasks:
                        print(f"[{self.id}] Reached max_tasks; exiting.")
                        os._exit(0)

                # attempt a steal only after handling a local task
                if src == 'local':
                    self.try_steal()
        threading.Thread(target=proc_loop, daemon=True).start()

    def try_steal(self):
        my_len = self.local_queue.qsize()
        my_cpu = psutil.cpu_percent(interval=0.1)
        for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1]):
            stub = self.peers.get(peer_id)
            if not stub: continue
            try:
                other_len = stub.GetQueueLength(tasks_pb2.Empty()).length
                other_cpu = stub.GetCPUUsage(tasks_pb2.Empty()).usage
                # steal only if they have more tasks AND lower CPU
                if other_len > my_len + 1 and other_cpu < my_cpu:
                    resp = stub.StealTask(tasks_pb2.Empty())
                    if resp.success:
                        self.local_queue.put((resp.task.name, resp.task.weight))
                        print(f"[{self.id}] Stole {resp.task.name} from server {peer_id}")
                        break
            except Exception as e:
                print(f"[{self.id}] Steal err→{peer_id}: {e}")

# --- server launcher ---
def serve(id, port, global_counter, max_tasks):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = NWRServer(id, port, global_counter, max_tasks)
    tasks_pb2_grpc.add_TaskServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[{id}] Listening on port {port}")
    # peer discovery
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
