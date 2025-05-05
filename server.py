#!/usr/bin/env python3
import os, time, threading, queue, psutil, grpc
from concurrent import futures
from multiprocessing import Value

import tasks_pb2, tasks_pb2_grpc
import json 
import collections

# Time between logging failures for the same peer
LOG_COOLDOWN = 5

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

# Helper function to create task result keys
TASK_KEY = lambda name: f"task::{name}" 

class NWRServer(tasks_pb2_grpc.TaskServiceServicer):
    def __init__(self, server_id, port, global_counter, max_tasks):
        self.id = server_id
        self.port = port
        self.max_tasks = max_tasks
        self.global_counter = global_counter

        # Separate queues:
        self.local_queue = queue.Queue()        # client+stolen tasks (stealable)
        self.rep_received_queue = queue.Queue()  # incoming replication (non-stealable)
        self.replication_queue = queue.Queue()   # tasks to send out

        self.data_store = {}
        self.peers = {}
        
        # Counts & timestamps for each peer's consecutive failures
        self.peer_failures = collections.defaultdict(lambda: {"count": 0, "last_log": 0.0})
        
        # Cache nearest peers for better performance
        self.nearest_peers = [pid for pid, _ in 
                              sorted(DISTANCES[self.id].items(), key=lambda x: x[1])]
                              
        self.lock = threading.Lock()

        print(f"[{self.id}] Initialized with empty queues.")

        # Start our two background loops
        self.start_replication_loop()
        self.start_processing()

    # ───── Peer health tracking helpers ─────────────────────────────────────────
    def _log_peer_down(self, pid: int):
        """Log a peer failure, with rate limiting to avoid log spam"""
        rec = self.peer_failures[pid]
        now = time.time()
        rec["count"] += 1
        if now - rec["last_log"] > LOG_COOLDOWN:
            print(f"[{self.id}] peer {pid} DOWN (consec={rec['count']})")
            rec["last_log"] = now

    def _reset_peer(self, pid: int):
        """Reset failure counter for a peer after successful communication"""
        self.peer_failures[pid]["count"] = 0

    # ───── RPC Methods ─────────────────────────────────────────
    def GetResult(self, req, context):
        """Get the result of a previously executed task"""
        key = TASK_KEY(req.name)
        # Check local storage first
        if key in self.data_store:
            return tasks_pb2.GetResultResponse(
                success=True, result=self.data_store[key], served_by=self.id
            )

        # Fall back to R-quorum fetch from nearest peers
        for peer_id in self.nearest_peers[:R-1]:
            stub = self.peers.get(peer_id)
            if not stub: 
                continue
            try:
                resp = stub.GetResult(req, timeout=0.8)
                if resp.success:
                    return resp
                self._reset_peer(peer_id)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    self._log_peer_down(peer_id)
                else:
                    raise  # Re-raise unexpected errors

        return tasks_pb2.GetResultResponse(success=False)

    def SendTask(self, req, context):
        """Handle both client submits and peer replication"""
        # 1. Finished-result fast-path (already executed)
        if getattr(req, "executed", False):
            self.data_store[TASK_KEY(req.name)] = req.result
            return tasks_pb2.Ack(success=True)

        # 2. Replicate descriptor immediately if this is a fresh client task
        if not req.replicated:
            for pid in self.nearest_peers[:W-1]:
                stub = self.peers.get(pid)
                if not stub:
                    continue
                try:
                    stub.SendTask(tasks_pb2.TaskRequest(
                        name=req.name,
                        weight=req.weight,
                        replicated=True,   # mark so peers don't re-replicate
                        source_id=self.id,
                        executed=False
                    ), timeout=0.8)
                    self._reset_peer(pid) 
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        self._log_peer_down(pid)
                    else:
                        raise

        # 3. Put in the right queue based on source
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
        """Report current queue length for load balancing"""
        return tasks_pb2.QueueLengthResponse(length=self.local_queue.qsize())

    def GetCPUUsage(self, req, ctx):
        """Report current CPU usage for load balancing"""
        usage = psutil.cpu_percent(interval=0.1)
        return tasks_pb2.CPUUsageResponse(usage=usage)

    def StealTask(self, req, ctx):
        """Allow peers to steal tasks for load balancing"""
        with self.lock:
            if not self.local_queue.empty():
                name, w = self.local_queue.get()
                print(f"[{self.id}] Providing STOLEN task {name} (w={w})")
                # Stolen tasks propagate as regular (replicated=False)
                return tasks_pb2.TaskResponse(
                    task=tasks_pb2.TaskRequest(name=name, weight=w, replicated=False, source_id=self.id),
                    success=True
                )
        return tasks_pb2.TaskResponse(success=False)

    def Heartbeat(self, req, ctx):
        """Simple health check method"""
        return tasks_pb2.HeartbeatResponse(timestamp=int(time.time()))

    def WriteData(self, req, ctx):
        """NWR write with quorum: local + W-1 nearest peers"""
        self.data_store[req.key] = req.value
        acks = 1
        
        # Try to write to W-1 nearest peers
        for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1])[:W-1]:
            stub = self.peers.get(peer_id)
            if not stub: 
                continue
            try:
                res = stub.WriteData(req)
                if res.success: 
                    acks += 1
                self._reset_peer(peer_id) 
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    self._log_peer_down(peer_id)  
                else:
                    raise  # Re-raise unexpected errors
        
        success = (acks >= W)
        print(f"[{self.id}] WriteData key={req.key} → success={success} (acks={acks})")
        return tasks_pb2.Ack(success=success)

    def ReadData(self, req, ctx):
        """NWR read with quorum: check local, then R-1 nearest peers"""
        if req.key in self.data_store:
            return tasks_pb2.ReadResponse(value=self.data_store[req.key], success=True, served_by=self.id)
        
        # Ask nearest peers up to R
        for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1]):
            stub = self.peers.get(peer_id)
            if not stub: 
                continue
            try:
                resp = stub.ReadData(req)
                if resp.success:
                    return tasks_pb2.ReadResponse(value=resp.value, success=True, served_by=peer_id)
                self._reset_peer(peer_id)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    self._log_peer_down(peer_id)
                else:
                    raise
        
        return tasks_pb2.ReadResponse(success=False)

    # ───── Background processing loops ─────────────────────────────────────────
    def start_replication_loop(self):
        """Start background thread for replicating tasks to peers"""
        def rep_loop():
            while True:
                name, w = self.replication_queue.get()
                
                # Replicate to W nearest peers
                for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1])[:W]:
                    stub = self.peers.get(peer_id)
                    if not stub: 
                        continue
                    try:
                        stub.SendTask(tasks_pb2.TaskRequest(
                            name=name, weight=w,
                            replicated=True, source_id=self.id,
                            executed=True
                        ))
                        print(f"[{self.id}] Replicated {name} → server {peer_id}")
                        self._reset_peer(peer_id) 
                    except grpc.RpcError as e:
                        if e.code() == grpc.StatusCode.UNAVAILABLE:
                            self._log_peer_down(peer_id)   
                        else:
                            print(f"[{self.id}] Replicate err→{peer_id}: {e}")
                
                self.replication_queue.task_done()

        threading.Thread(target=rep_loop, daemon=True).start()

    def start_processing(self):
        """Start background thread for processing tasks"""
        def proc_loop():
            while True:
                # Choose local first (stealable), then replicated
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
                # Simulate work
                for _ in range(w * 1_000_000): 
                    pass
                elapsed = time.time() - start
                print(f"[{self.id}] Executed {name} w={w} in {elapsed:.2f}s (src={src})")

                # Persist local result
                result_payload = json.dumps({"worker": self.id, "elapsed": elapsed})
                self.data_store[TASK_KEY(name)] = result_payload

                # Replicate the *result* to W-1 nearest peers
                for peer_id in self.nearest_peers[:W-1]:
                    stub = self.peers.get(peer_id)
                    if not stub: 
                        continue
                    try:
                        stub.SendTask(tasks_pb2.TaskRequest(
                            name=name,
                            weight=w,
                            replicated=True,
                            source_id=self.id,
                            executed=True,             # Flag indicating result
                            result=result_payload      # Actual result
                        ))
                        self._reset_peer(peer_id) 
                    except grpc.RpcError as e:
                        if e.code() == grpc.StatusCode.UNAVAILABLE:
                            self._log_peer_down(peer_id)
                        else:
                            raise

                # Add to replication queue for propagating to other servers
                if src == 'local':
                    self.replication_queue.put((name, w))

                # Count tasks and exit if max_tasks reached
                with self.global_counter.get_lock():
                    self.global_counter.value += 1
                    if self.global_counter.value >= self.max_tasks:
                        print(f"[{self.id}] Reached max_tasks; exiting.")
                        os._exit(0)

                # Attempt a steal only after handling a local task
                if src == 'local':
                    self.try_steal()

        threading.Thread(target=proc_loop, daemon=True).start()

    def try_steal(self):
        """Try to steal tasks from other servers for load balancing"""
        my_len = self.local_queue.qsize()
        my_cpu = psutil.cpu_percent(interval=0.1)
        
        # Try peers in order of distance (nearest first)
        for peer_id, _ in sorted(DISTANCES[self.id].items(), key=lambda x: x[1]):
            stub = self.peers.get(peer_id)
            if not stub: 
                continue
            try:
                other_len = stub.GetQueueLength(tasks_pb2.Empty()).length
                other_cpu = stub.GetCPUUsage(tasks_pb2.Empty()).usage
                
                # Steal only if they have more tasks AND lower CPU
                if other_len > my_len + 1 and other_cpu < my_cpu:
                    resp = stub.StealTask(tasks_pb2.Empty())
                    if resp.success:
                        self.local_queue.put((resp.task.name, resp.task.weight))
                        print(f"[{self.id}] Stole {resp.task.name} from server {peer_id}")
                        break
                self._reset_peer(peer_id) 
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    self._log_peer_down(peer_id)
                else:
                    print(f"[{self.id}] Steal err→{peer_id}: {e}")

# ───── Server launcher ─────────────────────────────────────────
def serve(id, port, global_counter, max_tasks):
    """Start server with given ID and port"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = NWRServer(id, port, global_counter, max_tasks)
    tasks_pb2_grpc.add_TaskServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[{id}] Listening on port {port}")
    
    # Set up peer connections
    for pid in range(N):
        if pid == id: 
            continue
        ch = grpc.insecure_channel(f"localhost:{50050+pid}")
        servicer.peers[pid] = tasks_pb2_grpc.TaskServiceStub(ch)
    
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