import multiprocessing
from multiprocessing import Process, cpu_count, current_process
import os
import time
import random
from utils import IsWinOS

class MultiProcess:
    def __init__(self):
        self.process_1 = None
        self.process_2 = None
        self.processes = []
        self.max_processes = cpu_count()
        print(f"Number of CPUs Available: {self.max_processes}")

    def firstTry(self):
        print(f"[PID {os.getpid()}] First Try Process Running.")
        time.sleep(1)
        return None

    def secondTry(self):
        print(f"[PID {os.getpid()}] Second Try Process Running.")
        time.sleep(1)
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
        return f"Process {index} result"

    def createProcessesBETA(self):
        print(f"Creating {self.max_processes} Processes (1 per CPU).")
        for i in range(self.max_processes):
            p = Process(target=self.worker, args=(i + 1,))
            self.processes.append(p)
            p.start()

        for p in self.processes:
            p.join()

        print("All Processes Have Been Finished.")

    def cpu_intensive_worker(self, number):
        """Worker para tarefas intensivas de CPU"""
        result = sum(i*i for i in range(number))
        print(f"[PID {os.getpid()}] Calculated sum of squares up to {number}: {result}")
        return result

    def createCPUIntensiveProcesses(self, numbers):
        print("Creating CPU-intensive processes...")
        processes = []
        
        for i, num in enumerate(numbers):
            p = Process(target=self.cpu_intensive_worker, args=(num,))
            processes.append(p)
            p.start()
        
        for p in processes:
            p.join()
        
        print("CPU-intensive processes finished.")

if __name__ == "__main__":
    """ Verificar se o Sistema Operativo é Windows """
    IsWinOS()

    mp = MultiProcess()
    print("=== Basic Multiprocessing Example ===")
    mp.createProcesses()
    
    print("\n=== CPU-Bound Processes Example ===")
    mp.createCPUIntensiveProcesses([1000000, 2000000, 3000000])