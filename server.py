### ✅ server.py
#!/usr/bin/env python3
import grpc, time, threading, queue, psutil, argparse, os, json
from concurrent import futures
import tasks_pb2, tasks_pb2_grpc

# This will hold distances for all known servers
global DISTANCES
DISTANCES = {
    0: {1:1, 2:2, 3:3, 4:4},
    1: {0:1, 2:1, 3:2, 4:3},
    2: {0:2, 1:1, 3:1, 4:2},
    3: {0:3, 1:2, 2:1, 4:1},
    4: {0:4, 1:3, 2:2, 3:1}
}

class NWRServer(tasks_pb2_grpc.TaskServiceServicer):
    def __init__(self, server_id, port, dist):
        self.id = server_id
        self.port = port
        self.peers = {}  # id → stub
        self.local_queue = queue.Queue()
        self.rep_queue = queue.Queue()
        self.lock = threading.Lock()
        self.distances = dist
        DISTANCES[self.id] = dist
        for peer_id in dist:
            if peer_id in DISTANCES:
                DISTANCES[peer_id][self.id] = dist[peer_id]

    def add_peer(self, peer_id, peer_host):
        if peer_id not in self.peers:
            ch = grpc.insecure_channel(peer_host)
            stub = tasks_pb2_grpc.TaskServiceStub(ch)
            self.peers[peer_id] = stub
            print(f"[{self.id}] Added peer {peer_id} at {peer_host}")

    def AnnouncePresence(self, req, ctx):
        print(f"[{self.id}] Received AnnouncePresence from {req.server_id}")
        self.add_peer(req.server_id, req.host)
        DISTANCES[self.id][req.server_id] = req.dist
        if req.server_id not in DISTANCES:
            DISTANCES[req.server_id] = {}
        DISTANCES[req.server_id][self.id] = req.dist
        return tasks_pb2.Ack(success=True)

    def SendTask(self, req, ctx):
        q = self.rep_queue if req.replicated else self.local_queue
        q.put((req.name, req.weight))
        print(f"[{self.id}] Received {'REPLICATED' if req.replicated else 'NEW'} task {req.name} (w={req.weight})")
        return tasks_pb2.Ack(success=True)

    def GetQueueLength(self, req, ctx):
        return tasks_pb2.QueueLengthResponse(length=self.local_queue.qsize())

    def GetCPUUsage(self, req, ctx):
        return tasks_pb2.CPUUsageResponse(usage=psutil.cpu_percent())

    def replicate(self, name, weight):
        for peer_id in list(self.peers.keys()):
            try:
                self.peers[peer_id].SendTask(tasks_pb2.TaskRequest(
                    name=name, weight=weight, replicated=True, source_id=self.id))
                print(f"[{self.id}] Replicated {name} → {peer_id}")
            except:
                print(f"[{self.id}] Failed to replicate to {peer_id}")

    def start_processing(self):
        def loop():
            while True:
                if not self.local_queue.empty():
                    name, w = self.local_queue.get()
                    print(f"[{self.id}] Processing {name} locally")
                    time.sleep(w / 100)
                    self.replicate(name, w)
                elif not self.rep_queue.empty():
                    name, w = self.rep_queue.get()
                    print(f"[{self.id}] Executing replicated task {name}")
                    time.sleep(w / 100)
                else:
                    time.sleep(0.5)
        threading.Thread(target=loop, daemon=True).start()

    def announce_to_peers(self):
        for peer_id, stub in self.peers.items():
            try:
                stub.AnnouncePresence(tasks_pb2.ServerInfo(
                    server_id=self.id,
                    host=f"localhost:{self.port}",
                    dist=self.distances[peer_id]
                ))
                print(f"[{self.id}] Announced to {peer_id}")
            except Exception as e:
                print(f"[{self.id}] Failed to announce to {peer_id}: {e}")

def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--dist', type=str, help='JSON distance map to peers')
    args = parser.parse_args()
    
    dist = json.loads(args.dist) if args.dist else {}
    server = NWRServer(args.id, args.port, dist)

    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    tasks_pb2_grpc.add_TaskServiceServicer_to_server(server, grpc_server)
    grpc_server.add_insecure_port(f'[::]:{args.port}')
    grpc_server.start()
    print(f"[{args.id}] Listening on port {args.port}")

    server.start_processing()
    time.sleep(2)
    server.announce_to_peers()
    grpc_server.wait_for_termination()

if __name__ == '__main__':
    serve()
