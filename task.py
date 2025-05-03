import time

class Task:
    def __init__(self, name, duration):
        self.name = name
        self.duration = duration

    def execute(self):
        print(f"Executing {self.name} for {self.duration:.2f}s...")
        time.sleep(self.duration)
        print(f"Completed {self.name}")