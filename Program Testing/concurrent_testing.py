import os
import time
import threading
from threading import Thread
from multiprocessing import Process, Queue, freeze_support, cpu_count

def IsWinOS():
    if os.name == "nt":
        freeze_support()
        print("Freeze Support Enabled Because of Windows Operating System.")
    else:
        print("Freeze Support Not Enabled Because of a Non Windows Operating System.")

class MultiThread:
    def __init__(self):
        self.thread_1 = None
        self.thread_2 = None

    def firstTry(self):
        print(f"[{threading.current_thread().name}] First Try Thread Running.")
        return None

    def secondTry(self):
        print(f"[{threading.current_thread().name}] Second Try Thread Running.")
        return None

    def createThreads(self):
        self.thread_1 = threading.Thread(target=self.firstTry, name="Thread First Try")
        self.thread_2 = threading.Thread(target=self.secondTry, name="Thread Second Try")

        self.thread_1.start()
        self.thread_2.start()

        self.thread_1.join()
        self.thread_2.join()

        print("All Threads Finished Successfully.")

class MultiProcess:
    def __init__(self):
        self.process_1 = None
        self.process_2 = None
        self.processes = []
        self.max_processes = cpu_count()
        print(f"Number of CPUs Available: {self.max_processes}")

    def firstTry(self):
        print(f"[{os.getpid()}] First Try Process Running.")
        return None

    def secondTry(self):
        print(f"[{os.getpid()}] Second Try Process Running.")
        return None

    def createProcesses(self):
        self.process_1 = Process(target=self.firstTry)
        self.process_2 = Process(target=self.secondTry)

        self.process_1.start()
        self.process_2.start()

        self.process_1.join()
        self.process_2.join()

        print("All Processes Finished Successfully.")

    def worker(self, index):
        print(f"[PID {os.getpid()}] Process {index} Started.")
        time.sleep(1)
        print(f"[PID {os.getpid()}] Process {index} Finished.")

    def createProcessesBETA(self):
        print(f"Creating {self.max_processes} Processes (1 per CPU).")
        for i in range(self.max_processes):
            p = Process(target=self.worker, args=(i + 1,))
            self.processes.append(p)
            p.start()

        for p in self.processes:
            p.join()

        print("All Processes Have Been Finished.")

if __name__ == "__main__":
    IsWinOS()
    MultiThread().createThreads()    
    MultiProcess().createProcesses()