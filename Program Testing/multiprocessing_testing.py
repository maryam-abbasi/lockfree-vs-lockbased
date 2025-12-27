import multiprocessing
from multiprocessing import Process, cpu_count, current_process
import os
import time
import random
from utils import IsWinOS

def worker(index):
    """ Função de worker para tarefas simples """
    start_time = time.time()
    print(f"[PID {os.getpid()}] Process {index} Started.")
    time.sleep(1)  ## Simulating a lighter task
    end_time = time.time() - start_time
    print(f"[PID {os.getpid()}] Process {index} Finished, Time Elapsed: {end_time:.2f} seconds")
    return f"Process {index} result"

def cpu_intensive_worker(number):
    """ Worker para tarefas intensivas de CPU (calcula soma dos quadrados) """
    start_time = time.time()
    result = sum(i * i for i in range(number))
    end_time = time.time() - start_time
    print(f"[PID {os.getpid()}] Calculated sum of squares up to {number}: {result} - Time elapsed: {end_time:.2f} seconds")
    return result

def create_processes(max_processes):
    """ Cria e executa os processos simples """
    print(f"Creating {max_processes} Processes (1 per CPU).")
    start_time = time.time()
    processes = []

    for i in range(max_processes):
        p = Process(target=worker, args=(i + 1,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    end_time = time.time() - start_time

    print(f"All processes have been finished, time elapsed: {end_time:.2f} seconds")

def create_cpu_intensive_processes(numbers):
    """ Cria e executa processos para tarefas intensivas de CPU """
    print("Creating CPU-intensive processes...")
    start_time = time.time()
    processes = []

    for num in numbers:
        p = Process(target=cpu_intensive_worker, args=(num,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    end_time = time.time() - start_time

    print(f"CPU-intensive processes finished, time elapsed: {end_time:.2f} seconds")

if __name__ == "__main__":
    """Check if the Operating System is Windows"""
    IsWinOS()

    max_processes = cpu_count()
    print(f"Number of CPUs Available: {max_processes}")

    print("\n=== Basic Multiprocessing Example ===")
    create_processes(max_processes)

    print("\n=== CPU-Bound Processes Example ===")
    create_cpu_intensive_processes([1000000, 2000000, 3000000]) 
