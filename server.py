#!/usr/bin/env python3
import os, time, threading, queue, psutil, grpc
import json, ast, collections, argparse
from concurrent import futures
from multiprocessing import Value

import tasks_pb2, tasks_pb2_grpc

LOG_COOLDOWN = 10
DISTANCES = {}  # will be populated dynamically
N, W, R = 5, 3, 2
TASK_KEY = lambda name: f"task::{name}"
DISTANCE_LOCK = threading.Lock()
SERVER_IPS = {
    0: "192.168.1.2",
    1: "192.168.1.2",
    2: "192.168.1.1",
    3: "192.168.1.1",
    4: "192.168.1.1",
}

class NWRServer(tasks_pb2_grpc.TaskServiceServicer):
    def __init__(self, server_id, port, global_counter, max_tasks, distances):
        self.id      = server_id
        self.port    = port
        self.global_counter = global_counter
        self.max_tasks      = max_tasks

        # merge in passed-in distances
        DISTANCES[self.id] = distances
        for p, d in distances.items():
            DISTANCES.setdefault(p, {})[self.id] = d

        self.local_queue        = queue.Queue()  # client + stolen tasks
        self.rep_received_queue = queue.Queue()  # incoming replicated tasks
        self.replication_queue  = queue.Queue()  # for propagating results

        self.data_store         = {}
        self.peers              = {}
        self.lock               = threading.Lock()
        self.failures           = collections.defaultdict(lambda:{"count":0,"last":0})

        print(f"[{self.id}] Initialized with distances: {distances}")

        self.start_replication_loop()
        self.start_processing()

    def _log_down(self, pid):
        rec = self.failures[pid]
        now = time.time(); rec["count"] += 1
        if now - rec["last"] > LOG_COOLDOWN:
            rec["last"] = now

    def _reset_peer(self, pid):
        self.failures[pid]["count"] = 0

    def add_peer(self, pid, host, dist):
        if pid not in self.peers:
            ch = grpc.insecure_channel(host)
            self.peers[pid] = tasks_pb2_grpc.TaskServiceStub(ch)
            print(f"[{self.id}] Added peer {pid} @ {host}")
            with DISTANCE_LOCK:
                DISTANCES[self.id][pid]     = dist
                DISTANCES.setdefault(pid,{})[self.id] = dist

    def AnnouncePresence(self, req, ctx):
        self.add_peer(req.server_id, req.host, req.dist)
        print(f"[{self.id}] Learned about server {req.server_id} @ {req.host} (dist={req.dist})")
        return tasks_pb2.Ack(success=True)

    def announce_to_peers(self):
        for pid, stub in list(self.peers.items()):
            try:
                stub.AnnouncePresence(tasks_pb2.ServerInfo(
                    server_id=self.id,
                    host=f"{SERVER_IPS[pid]}:{self.port}",
                    dist=DISTANCES[self.id][pid]
                ))
                print(f"[{self.id}] Announced to {pid}")
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    self._log_down(pid)

    def rank_peers(self, task_weight=0, purpose="steal"):
        ranked = []

        with DISTANCE_LOCK:
            distance_copy = dict(DISTANCES.get(self.id, {}))  # avoid iterating live dict

        for peer_id, dist in distance_copy.items():
            stub = self.peers.get(peer_id)
            if not stub:
                continue
            try:
                q_len = stub.GetQueueLength(tasks_pb2.Empty()).length
                cpu = stub.GetCPUUsage(tasks_pb2.Empty()).usage
                mem = psutil.virtual_memory().percent
                if purpose == "steal":
                    rank = (0.4 * dist) + (0.3 * q_len) + (0.2 * cpu) + (0.2 * mem) - (0.15 * task_weight)
                else:
                    rank = (0.4 * dist) - (0.3 * q_len) + (0.2 * cpu) + (0.2 * mem) - (0.15 * task_weight)
                ranked.append((peer_id, rank))
            except Exception as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    self._log_down(peer_id)

        ranked.sort(key=lambda x: x[1])
        return [peer_id for peer_id, _ in ranked]

    def SendTask(self, req, ctx):
        # result propagation
        if getattr(req, "executed", False):
            self.data_store[TASK_KEY(req.name)] = req.result
            return tasks_pb2.Ack(success=True)

        # initial replicate W-1
        if not req.replicated:
            for pid in self.rank_peers()[:W-1]:
                stub = self.peers.get(pid)
                if not stub: continue
                try:
                    stub.SendTask(tasks_pb2.TaskRequest(
                        name=req.name, weight=req.weight,
                        replicated=True, source_id=self.id,
                        executed=False, result=""
                    ), timeout=0.8)
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        self._log_down(pid)

        q = self.rep_received_queue if req.replicated else self.local_queue
        with self.lock:
            q.put((req.name, req.weight))
        tag = "REPLICATED" if req.replicated else "NEW"
        print(f"[{self.id}] Received {tag} task {req.name} w={req.weight}")
        return tasks_pb2.Ack(success=True)

    def GetResult(self, req, ctx):
        key = TASK_KEY(req.name)
        if key in self.data_store:
            return tasks_pb2.GetResultResponse(success=True, result=self.data_store[key], served_by=self.id)
        for pid in self.rank_peers(purpose="replication")[:R]:
            stub = self.peers.get(pid)
            if not stub: continue
            try:
                resp = stub.GetResult(req, timeout=0.8)
                if resp.success: return resp
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    self._log_down(pid)
        return tasks_pb2.GetResultResponse(success=False)

    def WriteData(self, req, ctx):
        self.data_store[req.key] = req.value
        acks = 1
        for pid in self.rank_peers(task_weight=0, purpose="replication")[:W-1]:
            stub = self.peers.get(pid)
            if not stub: continue
            try:
                res = stub.WriteData(req)
                if res.success: acks += 1
            except grpc.RpcError:
                self._log_down(pid)
        success = (acks >= W)
        print(f"[{self.id}] WriteData key={req.key} → success={success} (acks={acks})")
        return tasks_pb2.Ack(success=success)

    def ReadData(self, req, ctx):
        if req.key in self.data_store:
            return tasks_pb2.ReadResponse(value=self.data_store[req.key], success=True, served_by=self.id)
        for pid in self.rank_peers(purpose="replication"):
            stub = self.peers.get(pid)
            if not stub: continue
            try:
                resp = stub.ReadData(req)
                if resp.success: return resp
            except grpc.RpcError:
                self._log_down(pid)
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
                    task=tasks_pb2.TaskRequest(
                        name=name, weight=w,
                        replicated=False, source_id=self.id,
                        executed=False, result=""
                    ),
                    success=True
                )
        return tasks_pb2.TaskResponse(success=False)

    def Heartbeat(self, req, ctx):
        return tasks_pb2.HeartbeatResponse(timestamp=int(time.time()))

    def start_replication_loop(self):
        def loop():
            while True:
                name, w = self.replication_queue.get()
                for pid in self.rank_peers(task_weight=w, purpose="replication")[:W-1]:
                    stub = self.peers.get(pid)
                    if not stub: continue
                    try:
                        stub.SendTask(tasks_pb2.TaskRequest(
                            name=name, weight=w,
                            replicated=True, source_id=self.id,
                            executed=True,
                            result=self.data_store[TASK_KEY(name)]
                        ))
                        print(f"[{self.id}] Replicated {name} → server {pid}")
                        self._reset_peer(pid)
                    except grpc.RpcError as e:
                        if e.code() == grpc.StatusCode.UNAVAILABLE:
                            self._log_down(pid)
                self.replication_queue.task_done()
        threading.Thread(target=loop, daemon=True).start()

    def start_processing(self):
        def loop():
            while True:
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

                payload = json.dumps({"worker": self.id, "elapsed": elapsed})
                self.data_store[TASK_KEY(name)] = payload

                if src == "local":
                    self.replication_queue.put((name, w))

                with self.global_counter.get_lock():
                    self.global_counter.value += 1
                    if self.global_counter.value >= self.max_tasks:
                        os._exit(0)

                if src == "local":
                    self.try_steal()
        threading.Thread(target=loop, daemon=True).start()

    def try_steal(self):
        my_len = self.local_queue.qsize()
        my_cpu = psutil.cpu_percent(interval=0.1)

        for pid in self.rank_peers(task_weight=0, purpose="steal"):
            stub = self.peers.get(pid)
            if not stub:
                continue

            print(f"[{self.id}] Attempting to steal from server {pid}...")

            try:
                other_len = stub.GetQueueLength(tasks_pb2.Empty()).length
                other_cpu = stub.GetCPUUsage(tasks_pb2.Empty()).usage

                print(f"[{self.id}] Comparison with server {pid} → "
                  f"my_len={my_len}, other_len={other_len}, "
                  f"my_cpu={my_cpu:.2f}%, other_cpu={other_cpu:.2f}%")

                if other_len > my_len + 1 and other_cpu < my_cpu:
                    resp = stub.StealTask(tasks_pb2.Empty())
                    if resp.success:
                        self.local_queue.put((resp.task.name, resp.task.weight))
                        print(f"[{self.id}] Stole {resp.task.name} from server {pid}")
                        break
                    else:
                        print(f"[{self.id}] No task to steal from server {pid}")
            except grpc.RpcError:
                self._log_down(pid)


def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id',        type=int, required=True)
    parser.add_argument('--port',      type=int, required=True)
    parser.add_argument('--max_tasks', type=int, default=100)
    parser.add_argument('--distances', type=str, default='{}',
                        help='JSON or Python dict literal')
    args = parser.parse_args()

    try:
        distances = json.loads(args.distances)
    except Exception:
        distances = ast.literal_eval(args.distances)

    gc = Value('i',0)
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    svc = NWRServer(args.id, args.port, gc, args.max_tasks, distances)
    tasks_pb2_grpc.add_TaskServiceServicer_to_server(svc, grpc_server)
    grpc_server.add_insecure_port(f"[::]:{args.port}")
    grpc_server.start()
    print(f"[{args.id}] Listening on port {args.port}")

    # connect to known peers
    for pid in distances:
        if pid == args.id: continue
        host = f"{SERVER_IPS[pid]}:{50050+pid}"
        svc.peers[pid] = tasks_pb2_grpc.TaskServiceStub(grpc.insecure_channel(host))

    svc.announce_to_peers()
    grpc_server.wait_for_termination()

if __name__ == '__main__':
    serve()
