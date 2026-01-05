import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import time
import random
import os
from utils import IsWinOS

class ProcessPoolExample:
    def __init__(self):
        self.max_workers = multiprocessing.cpu_count()

    def cpu_intensive_task(self, number):
        """CPU-intensive task (ideal for multiprocessing)"""
        print(f"[PID {os.getpid()}] Processing number: {number}")
        result = sum(i * i for i in range(number))
        return f"PID {os.getpid()}: sum(i^2) for i<{number} = {result}"

    def run_cpu_intensive_tasks(self, numbers):
        """Executes CPU-intensive tasks"""
        print(f"Running {len(numbers)} CPU-intensive tasks with {self.max_workers} workers...")

        start_time = time.time()
        
        # Moving executor creation outside of the class to avoid pickling issues
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.cpu_intensive_task, num): num for num in numbers}
            results = []
            
            for future in as_completed(futures):
                num = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    print(f"Completed: {result}")
                except Exception as e:
                    print(f"Task for {num} failed: {e}")

        end_time = time.time() - start_time
        print(f"All CPU Intentive Tasks Finished - Time elapsed: {end_time:.2f} seconds")
        
        return results

    def mixed_task(self, task_id):
        """Mixed task (CPU + I/O)"""
        print(f"[PID {os.getpid()}] Mixed task {task_id} started")
        
        # Simulate a CPU-bound task
        cpu_result = sum(i * i for i in range(100000))
        
        # Simulate an I/O-bound task
        time.sleep(random.uniform(0.1, 0.3))
        
        result = f"Task {task_id} completed (CPU: {cpu_result})"
        print(f"[PID {os.getpid()}] {result}")
        return result

    def run_mixed_tasks(self, num_tasks=8):
        """Executes mixed tasks"""
        print(f"Running {num_tasks} mixed tasks...")

        start_time = time.time()
        
        # Moving executor creation outside of the class to avoid pickling issues
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.mixed_task, i+1) for i in range(num_tasks)]
            
            results = []
            for future in as_completed(futures):
                results.append(future.result())

        end_time = time.time() - start_time

        print(f"All Mixed Tasks Finished - Time elapsed: {end_time:.2f} seconds")
        
        return results

    def parallel_search(self, data_chunk, target):
        """Parallel search in data chunks"""
        print(f"[PID {os.getpid()}] Searching in chunk of size {len(data_chunk)}")
        time.sleep(0.1)
        if target in data_chunk:
            return f"Found {target} in chunk: {data_chunk}"
        return f"Target {target} not found in chunk"

    def run_parallel_search(self, data, target, chunk_size=3):
        """Executes parallel search"""

        start_time = time.time()
        
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        print(f"Searching for '{target}' in {len(chunks)} chunks")
        
        # Moving executor creation outside of the class to avoid pickling issues
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.parallel_search, chunk, target) for chunk in chunks]
            
            results = []
            for future in as_completed(futures):
                results.append(future.result())

        end_time = time.time() - start_time

        print(f"All Parallel Searches Finished - Time elasped: {end_time:.2f} seconds")
        
        return results

    def shutdown(self):
        """Shuts down the executor"""
        print("ProcessPoolExecutor shutdown")

if __name__ == "__main__":
    """Check if the Operating System is Windows"""
    IsWinOS()

    ppe = ProcessPoolExample()

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
