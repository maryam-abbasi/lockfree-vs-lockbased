import threading
from threading import Thread, current_thread
import os
import time
import random
from utils import IsWinOS

class MultiThread:
    def __init__(self):
        self.threads = []

    def worker(self, index):
        start_time = time.time()
        print(f"[{current_thread().name}] Worker {index} started")
        rand_max = random.randint(1000000, 4000000)
        result = sum(range(1, rand_max))
        print(f"Task {index}: Sum is {result}")
        end_time = time.time() - start_time
        print(f"[{current_thread().name}] Worker {index} finished - Time elapsed for Thread {index}: {end_time:.2f} seconds")
        return f"Result from worker {index}: {result}"

    def createMultipleThreads(self, num_threads=5):
        start_all_time = time.time()
        print()
        print(50*"=")
        print(f"Creating {num_threads} threads...")
        print()
        
        for i in range(num_threads):
            thread = Thread(target=self.worker, args=(i+1,), name=f"Worker-{i+1}")
            self.threads.append(thread)
            thread.start()

        for thread in self.threads:
            thread.join()

        end_all_time = time.time() - start_all_time
        print()
        print(f"All Multiple Threads Finished Successfully - Time elapsed: {end_all_time:.2f} seconds")
        print(50*"=")
        print()

if __name__ == "__main__":
    """Check if the Operating System is Windows"""
    IsWinOS()

    print("\n=== Multiple Threads Example ===")
    mt = MultiThread()
    mt.createMultipleThreads(4) ## 4 Threads
    mt.createMultipleThreads(8) ## 8 Threads
    mt.createMultipleThreads(16) ## 16 Threads