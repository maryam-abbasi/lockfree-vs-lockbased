import threading
from threading import Thread, current_thread
import os
import time
import random
from utils import IsWinOS

class MultiThread:
    def __init__(self):
        self.thread_1 = None
        self.thread_2 = None
        self.threads = []

    def firstTry(self):
        print(f"[{current_thread().name}] First Try Thread Running.")
        time.sleep(1)
        return None

    def secondTry(self):
        print(f"[{current_thread().name}] Second Try Thread Running.")
        time.sleep(1)
        return None

    def createThreads(self):
        self.thread_1 = Thread(target=self.firstTry, name="Thread First Try")
        self.thread_2 = Thread(target=self.secondTry, name="Thread Second Try")

        self.thread_1.start()
        self.thread_2.start()

        self.thread_1.join()
        self.thread_2.join()

        print("All Threads Finished Successfully.")

    def worker(self, index, duration):
        print(f"[{current_thread().name}] Worker {index} started, sleeping {duration}s")
        time.sleep(duration)
        print(f"[{current_thread().name}] Worker {index} finished")
        return f"Result from worker {index}"

    def createMultipleThreads(self, num_threads=5):
        print(f"Creating {num_threads} threads...")
        
        for i in range(num_threads):
            duration = random.uniform(0.5, 2.0)
            thread = Thread(target=self.worker, args=(i+1, duration), name=f"Worker-{i+1}")
            self.threads.append(thread)
            thread.start()

        for thread in self.threads:
            thread.join()

        print("All Multiple Threads Finished Successfully.")

if __name__ == "__main__":
    """ Verificar se o Sistema Operativo é Windows"""
    IsWinOS()

    mt = MultiThread()
    print("=== Basic Threading Example ===")
    mt.createThreads()
    
    print("\n=== Multiple Threads Example ===")
    mt.createMultipleThreads(4)