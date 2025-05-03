import grpc
import time
import random
import threading
import queue
import psutil
import argparse

from concurrent import futures
import taskstealing_pb2
import taskstealing_pb2_grpc
from task import Task
from distance import DISTANCES
import taskstealing_pb2_grpc
from grpc import insecure_channel
from taskstealing_pb2 import Task as ProtoTask
import multiprocessing
from multiprocessing import Value

class TaskStealingServer(taskstealing_pb2_grpc.TaskStealingServiceServicer):
    def __init__(self, server_id, global_counter, max_tasks):
        self.global_counter = global_counter
        self.max_tasks = max_tasks
        self.id = server_id
        self.task_queue = queue.Queue()
        if self.id == 2:
            print(f"[Server {self.id}] started with NO initial tasks to test stealing.")
        else:
            for i in range(random.randint(6, 10)):
                self.task_queue.put(Task(f"Task_{self.id}_{i}", random.uniform(0.5, 1.5)))

        self.lock = threading.Lock()
        self.peers = {}

    def GetQueueLength(self, request, context):
        with self.lock:
            return taskstealing_pb2.QueueLengthResponse(length=self.task_queue.qsize())
    
    def update_global_counter():
        with FileLock("global_counter.txt.lock"):
            with open("global_counter.txt", "r+") as f:
                count = int(f.read())
                count += 1
                f.seek(0)
                f.write(str(count))
                f.truncate()
            return count

    def GetCPUUsage(self, request, context):
        usage = psutil.cpu_percent(interval=0.1)
        return taskstealing_pb2.CPUUsageResponse(usage=usage)

    def StealTask(self, request, context):
        with self.lock:
            if not self.task_queue.empty():
                task = self.task_queue.get()
                return taskstealing_pb2.TaskResponse(
                    task=ProtoTask(name=task.name, duration=task.duration),
                    success=True
                )
        return taskstealing_pb2.TaskResponse(success=False)

    def SendTask(self, request, context):
        with self.lock:
            task = Task(request.name, request.duration)
            self.task_queue.put(task)
            return taskstealing_pb2.Ack(success=True)

    def start_processing(self):
        def process_loop():
            while True:
                if not self.task_queue.empty():
                    task = self.task_queue.get()
                    task.execute()
                    with self.global_counter.get_lock():
                        self.global_counter.value += 1
                    print(f"[Server {self.id}] Total tasks done: {self.global_counter.value}")
                    if self.global_counter.value >= self.max_tasks:
                        print(f"[Server {self.id}] Reached task limit. Shutting down.")
                        os._exit(0)
                    else:
                        self.try_to_steal()
                    time.sleep(1)

        t = threading.Thread(target=process_loop, daemon=True)
        t.start()

    def try_to_steal(self):

        neighbors = sorted(DISTANCES[self.id].items(), key=lambda x: x[1])
        close = [n[0] for n in neighbors[:2]]
        far = [n[0] for n in neighbors[2:4]]
        to_try = close + far

        my_cpu = psutil.cpu_percent(interval=0.1)
        my_len = self.task_queue.qsize()

        for peer_id in to_try:
            if peer_id not in self.peers:
                continue

            stub = self.peers[peer_id]
            try:
                other_len = stub.GetQueueLength(taskstealing_pb2.Empty()).length
                other_cpu = stub.GetCPUUsage(taskstealing_pb2.Empty()).usage

                print(f"[Server {self.id}] trying to steal from {peer_id} | "
                  f"my_len={my_len}, other_len={other_len}, "
                  f"my_cpu={my_cpu:.2f}, other_cpu={other_cpu:.2f}")

                if my_len < 5:
                    can_steal = False
                    if other_len > 5 or (my_len == 0 and other_len >= 2):
                        if my_cpu < other_cpu:
                            can_steal = True
                    elif my_len < other_len and not (my_len == 2 and other_len == 3):
                        if my_cpu < other_cpu:
                            can_steal = True

                    if can_steal:
                        response = stub.StealTask(taskstealing_pb2.Empty())
                        if response.success:
                            self.task_queue.put(Task(response.task.name, response.task.duration))
                            print(f"[Server {self.id}] stole {response.task.name} from {peer_id}")
                            return  # Exit stealing after one successful steal

            except Exception as e:
                print(f"[Server {self.id}] error contacting {peer_id}: {e}")
                continue


    def _attempt_steal(self, stub, peer_id):
        response = stub.StealTask(taskstealing_pb2.Empty())
        if response.success:
            self.task_queue.put(Task(response.task.name, response.task.duration))
            print(f"[Server {self.id}] stole {response.task.name} from {peer_id}")


def serve(server_id, port, global_counter, max_tasks):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ts_server = TaskStealingServer(server_id, global_counter, max_tasks)
    taskstealing_pb2_grpc.add_TaskStealingServiceServicer_to_server(ts_server, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Server {server_id} started on port {port}")

    # Connect to peers
    for i in range(5):
        if i == server_id:
            continue
        peer_port = 50050 + i
        channel = grpc.insecure_channel(f"localhost:{peer_port}")
        stub = taskstealing_pb2_grpc.TaskStealingServiceStub(channel)
        ts_server.peers[i] = stub

    ts_server.start_processing()
    server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument('--max_tasks', type=int, default=100)
    args = parser.parse_args()

    global_counter = multiprocessing.Value("i", 0)  # shared integer
    serve(args.id, args.port, global_counter, args.max_tasks)