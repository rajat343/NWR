#!/usr/bin/env python3
import os, time, threading, queue, psutil, grpc
import json, collections
from concurrent import futures
from multiprocessing import Value

import tasks_pb2, tasks_pb2_grpc

# ──────────────────────────────────────────────────────────────
LOG_COOLDOWN = 5
DISTANCES = {
    0:{1:1,2:2,3:3,4:4}, 1:{0:1,2:1,3:2,4:3},
    2:{0:2,1:1,3:1,4:2}, 3:{0:3,1:2,2:1,4:1},
    4:{0:4,1:3,2:2,3:1}
}
N, W, R = 5, 3, 2
TASK_KEY = lambda name: f"task::{name}"
# ──────────────────────────────────────────────────────────────

class NWRServer(tasks_pb2_grpc.TaskServiceServicer):
    def __init__(self, server_id, port, global_counter, max_tasks):
        self.id      = server_id
        self.global_counter = global_counter
        self.max_tasks      = max_tasks

        self.local_queue        = queue.Queue()  # client + stolen tasks
        self.rep_received_queue = queue.Queue()  # incoming replicated tasks
        self.replication_queue  = queue.Queue()  # for propagating results

        self.data_store    = {}
        self.peers         = {}
        self.lock          = threading.Lock()
        self.peer_failures = collections.defaultdict(lambda:{"count":0,"last_log":0.0})
        self.nearest_peers = [pid for pid,_ in sorted(DISTANCES[self.id].items(), key=lambda x:x[1])]

        print(f"[{self.id}] Initialized.")

        self.start_replication_loop()
        self.start_processing()

    def _log_peer_down(self, pid):
        rec = self.peer_failures[pid]
        now = time.time(); rec["count"]+=1
        if now - rec["last_log"] > LOG_COOLDOWN:
            print(f"[{self.id}] peer {pid} DOWN (consec={rec['count']})")
            rec["last_log"] = now

    def _reset_peer(self, pid):
        self.peer_failures[pid]["count"] = 0

    def rank_peers(self):
        ranked = []
        for pid, dist in DISTANCES[self.id].items():
            stub = self.peers.get(pid)
            if not stub: continue
            try:
                qlen = stub.GetQueueLength(tasks_pb2.Empty()).length
                cpu  = stub.GetCPUUsage(tasks_pb2.Empty()).usage
                mem  = psutil.virtual_memory().percent
                score = 0.4*dist + 0.2*qlen + 0.2*cpu + 0.2*mem
                ranked.append((pid, score))
                self._reset_peer(pid)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    self._log_peer_down(pid)
        ranked.sort(key=lambda x:x[1])
        return [pid for pid,_ in ranked]

    # ── RPC METHODS ───────────────────────────────────────────────
    def SendTask(self, req, ctx):
        # result‐propagation fast path
        if getattr(req, "executed", False):
            self.data_store[TASK_KEY(req.name)] = req.result
            return tasks_pb2.Ack(success=True)

        # initial W−1 replication for new tasks
        if not req.replicated:
            for pid in self.rank_peers()[:W-1]:
                stub = self.peers.get(pid)
                if not stub: continue
                try:
                    stub.SendTask(tasks_pb2.TaskRequest(
                        name=req.name, weight=req.weight,
                        replicated=True, source_id=self.id,
                        executed=False
                    ), timeout=0.8)
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        self._log_peer_down(pid)

        # enqueue into the appropriate queue
        queue_ = self.rep_received_queue if req.replicated else self.local_queue
        with self.lock:
            queue_.put((req.name, req.weight))
        tag = "REPLICATED" if req.replicated else "NEW"
        print(f"[{self.id}] Received {tag} task {req.name} w={req.weight}")
        return tasks_pb2.Ack(success=True)

    def GetResult(self, req, ctx):
        key = TASK_KEY(req.name)
        if key in self.data_store:
            return tasks_pb2.GetResultResponse(success=True, result=self.data_store[key], served_by=self.id)
        for pid in self.rank_peers()[:R]:
            stub = self.peers.get(pid)
            if not stub: continue
            try:
                resp = stub.GetResult(req, timeout=0.8)
                if resp.success: return resp
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    self._log_peer_down(pid)
        return tasks_pb2.GetResultResponse(success=False)

    def WriteData(self, req, ctx):
        self.data_store[req.key] = req.value
        acks = 1
        for pid in self.rank_peers()[:W-1]:
            stub = self.peers.get(pid)
            if not stub: continue
            try:
                res = stub.WriteData(req)
                if res.success: acks += 1
            except grpc.RpcError:
                self._log_peer_down(pid)
        success = (acks >= W)
        print(f"[{self.id}] WriteData key={req.key} → success={success} (acks={acks})")
        return tasks_pb2.Ack(success=success)

    def ReadData(self, req, ctx):
        if req.key in self.data_store:
            return tasks_pb2.ReadResponse(value=self.data_store[req.key], success=True, served_by=self.id)
        for pid in self.rank_peers():
            stub = self.peers.get(pid)
            if not stub: continue
            try:
                resp = stub.ReadData(req)
                if resp.success: return resp
            except grpc.RpcError:
                self._log_peer_down(pid)
        return tasks_pb2.ReadResponse(success=False)

    def GetQueueLength(self, req, ctx):
        return tasks_pb2.QueueLengthResponse(length=self.local_queue.qsize())

    def GetCPUUsage(self, req, ctx):
        return tasks_pb2.CPUUsageResponse(usage=psutil.cpu_percent(interval=0.1))

    def StealTask(self, req, ctx):
        with self.lock:
            if not self.local_queue.empty():
                name, w = self.local_queue.get()
                print(f"[{self.id}] Providing STOLEN task {name} (w={w})")
                return tasks_pb2.TaskResponse(
                    task=tasks_pb2.TaskRequest(name=name, weight=w, replicated=False, source_id=self.id),
                    success=True
                )
        return tasks_pb2.TaskResponse(success=False)

    def Heartbeat(self, req, ctx):
        return tasks_pb2.HeartbeatResponse(timestamp=int(time.time()))

    # ── BACKGROUND LOOPS ──────────────────────────────────────────
    def start_replication_loop(self):
        def loop():
            while True:
                name, w = self.replication_queue.get()
                for pid in self.rank_peers()[:W-1]:
                    stub = self.peers.get(pid)
                    if not stub: continue
                    try:
                        stub.SendTask(tasks_pb2.TaskRequest(
                            name=name, weight=w,
                            replicated=True, source_id=self.id,
                            executed=True,
                            result=self.data_store[TASK_KEY(name)]
                        ))
                    except grpc.RpcError as e:
                        if e.code() == grpc.StatusCode.UNAVAILABLE:
                            self._log_peer_down(pid)
                self.replication_queue.task_done()
        threading.Thread(target=loop, daemon=True).start()

    def start_processing(self):
        def loop():
            while True:
                # **CHECK REPLICATED TASKS FIRST**
                if not self.rep_received_queue.empty():
                    src = "rep"
                    name, w = self.rep_received_queue.get()
                elif not self.local_queue.empty():
                    src = "local"
                    name, w = self.local_queue.get()
                else:
                    time.sleep(0.2)
                    continue

                start = time.time()
                for _ in range(w * 1_000_000):
                    pass
                elapsed = time.time() - start
                print(f"[{self.id}] Executed {name} w={w} in {elapsed:.2f}s (src={src})")

                # store result
                payload = json.dumps({"worker": self.id, "elapsed": elapsed})
                self.data_store[TASK_KEY(name)] = payload

                # only local tasks trigger result propagation
                if src == "local":
                    self.replication_queue.put((name, w))

                # global count check
                with self.global_counter.get_lock():
                    self.global_counter.value += 1
                    if self.global_counter.value >= self.max_tasks:
                        os._exit(0)

                # only steal after handling local work
                if src == "local":
                    self.try_steal()
        threading.Thread(target=loop, daemon=True).start()

    def try_steal(self):
        my_len = self.local_queue.qsize()
        my_cpu = psutil.cpu_percent(interval=0.1)
        for pid in self.rank_peers():
            stub = self.peers.get(pid)
            if not stub: continue
            try:
                other_len = stub.GetQueueLength(tasks_pb2.Empty()).length
                other_cpu = stub.GetCPUUsage(tasks_pb2.Empty()).usage
                if other_len > my_len + 1 and other_cpu < my_cpu:
                    resp = stub.StealTask(tasks_pb2.Empty())
                    if resp.success:
                        self.local_queue.put((resp.task.name, resp.task.weight))
                        print(f"[{self.id}] Stole {resp.task.name} from {pid}")
                        break
            except grpc.RpcError:
                self._log_peer_down(pid)

# ── SERVER LAUNCHER ──────────────────────────────────────────────────────
def serve(id, port, global_counter, max_tasks):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    svc = NWRServer(id, port, global_counter, max_tasks)
    tasks_pb2_grpc.add_TaskServiceServicer_to_server(svc, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[{id}] Listening on {port}")

    # connect to peers
    for pid in range(N):
        if pid == id: continue
        ch = grpc.insecure_channel(f"localhost:{50050+pid}")
        svc.peers[pid] = tasks_pb2_grpc.TaskServiceStub(ch)

    server.wait_for_termination()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--id',   type=int, required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--max_tasks', type=int, default=100)
    args = parser.parse_args()
    gc = Value('i', 0)
    serve(args.id, args.port, gc, args.max_tasks)
