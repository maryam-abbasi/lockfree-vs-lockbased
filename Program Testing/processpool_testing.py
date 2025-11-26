import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import time
import random
import os
from utils import IsWinOS

class ProcessPoolExample:
    def __init__(self, max_workers=None):
        self.max_workers = max_workers or multiprocessing.cpu_count()
        self.executor = ProcessPoolExecutor(max_workers=self.max_workers)

    def cpu_intensive_task(self, number):
        """Tarefa intensiva em CPU (ideal para multiprocessing)"""
        print(f"[PID {os.getpid()}] Processing number: {number}")
        result = sum(i * i for i in range(number))
        return f"PID {os.getpid()}: sum(i^2) for i<{number} = {result}"

    def run_cpu_intensive_tasks(self, numbers):
        """Executa tarefas CPU-intensive"""
        print(f"Running {len(numbers)} CPU-intensive tasks with {self.max_workers} workers...")
        
        futures = {self.executor.submit(self.cpu_intensive_task, num): num for num in numbers}
        results = []
        
        for future in as_completed(futures):
            num = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"Completed: {result}")
            except Exception as e:
                print(f"Task for {num} failed: {e}")
        
        return results

    def mixed_task(self, task_id):
        """Tarefa mista (CPU + I/O)"""
        print(f"[PID {os.getpid()}] Mixed task {task_id} started")
        
        ## Simulação de Ação CPU-Bound
        cpu_result = sum(i * i for i in range(100000))
        
        ## Simulação de Parte Input/Output
        time.sleep(random.uniform(0.1, 0.3))
        
        result = f"Task {task_id} completed (CPU: {cpu_result})"
        print(f"[PID {os.getpid()}] {result}")
        return result

    def run_mixed_tasks(self, num_tasks=8):
        """Executa tarefas mistas"""
        print(f"Running {num_tasks} mixed tasks...")
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.mixed_task, i+1) for i in range(num_tasks)]
            
            results = []
            for future in as_completed(futures):
                results.append(future.result())
        
        return results

    def parallel_search(self, data_chunk, target):
        """Busca paralela em chunks de dados"""
        print(f"[PID {os.getpid()}] Searching in chunk of size {len(data_chunk)}")
        time.sleep(0.1)
        if target in data_chunk:
            return f"Found {target} in chunk: {data_chunk}"
        return f"Target {target} not found in chunk"

    def run_parallel_search(self, data, target, chunk_size=3):
        """Executa busca paralela"""
        
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        print(f"Searching for '{target}' in {len(chunks)} chunks")
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.parallel_search, chunk, target) for chunk in chunks]
            
            results = []
            for future in as_completed(futures):
                results.append(future.result())
        
        return results

    def shutdown(self):
        """Encerra o executor"""
        self.executor.shutdown(wait=True)
        print("ProcessPoolExecutor shutdown")

if __name__ == "__main__":
    """ Verificar se o Sistema Operativo é Windows """
    IsWinOS()

    ppe = ProcessPoolExample(max_workers=4)
    
    print("=== CPU-Intensive Tasks ===")
    numbers = [1000000, 1500000, 2000000, 1200000, 1800000]
    results = ppe.run_cpu_intensive_tasks(numbers)
    
    print("\n=== Mixed Tasks ===")
    ppe.run_mixed_tasks(6)
    
    print("\n=== Parallel Search ===")
    data = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape"]
    search_results = ppe.run_parallel_search(data, "cherry", chunk_size=2)
    for result in search_results:
        print(result)
    
    ppe.shutdown()