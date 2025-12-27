import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import threading
from utils import IsWinOS

class ThreadPoolExample:
    def __init__(self, max_workers=None):
        self.max_workers = max_workers or 4
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)

    def simple_task(self, task_id, duration):
        """Simple task for demonstration"""
        print(f"[{threading.current_thread().name}] Task {task_id} started, sleeping {duration}s")
        time.sleep(duration)
        result = f"Task {task_id} completed after {duration}s"
        print(f"[{threading.current_thread().name}] {result}")
        return result

    def run_simple_tasks(self, num_tasks=8):
        """Executes multiple simple tasks"""
        print(f"Running {num_tasks} tasks with {self.max_workers} workers...")
        
        futures = []
        for i in range(num_tasks):
            duration = random.uniform(0.5, 2.0)
            future = self.executor.submit(self.simple_task, i+1, duration)
            futures.append(future)

        results = []
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Task failed: {e}")

        print(f"All tasks completed. Results: {results}")
        return results

    def cpu_bound_task(self, number):
        """CPU-bound task (not ideal for threading)"""
        return sum(i * i for i in range(number))

    def io_bound_task(self, task_id):
        """I/O-bound task (ideal for threading)"""
        print(f"[{threading.current_thread().name}] IO Task {task_id} started")
        time.sleep(random.uniform(0.5, 1.5))  # Simulating Input/Output (I/O)
        result = f"IO Task {task_id} completed"
        print(f"[{threading.current_thread().name}] {result}")
        return result

    def run_io_bound_tasks(self, num_tasks=10):
        """Executes I/O-bound tasks"""
        print(f"Running {num_tasks} I/O bound tasks...")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.io_bound_task, i+1) for i in range(num_tasks)]
            
            results = []
            for future in as_completed(futures):
                results.append(future.result())
        
        return results

    def process_with_timeout(self, task_id, duration, timeout=1.0):
        """Task with timeout"""
        try:
            future = self.executor.submit(self.simple_task, task_id, duration)
            result = future.result(timeout=timeout)
            return f"SUCCESS: {result}"
        except concurrent.futures.TimeoutError:
            return f"TIMEOUT: Task {task_id} exceeded {timeout}s"

    def run_tasks_with_timeouts(self):
        """Executes tasks with timeouts"""
        tasks = [
            # Format of the object:
            # task_id, duration, timeout
            (1, 0.5, 1.0),   
            (2, 1.5, 1.0),
            (3, 0.8, 1.0),
            (4, 2.0, 1.0),
        ]
        
        results = []
        for task_id, duration, timeout in tasks:
            result = self.process_with_timeout(task_id, duration, timeout)
            results.append(result)
            print(result)
        
        return results

    def shutdown(self):
        """Shuts down the executor"""
        self.executor.shutdown(wait=True)
        print("ThreadPoolExecutor shutdown")

if __name__ == "__main__":
    """Check if the Operating System is Windows"""
    IsWinOS()

    tpe = ThreadPoolExample(max_workers=3)
    
    print("=== Simple Thread Pool Tasks ===")
    tpe.run_simple_tasks(6)
    
    print("\n=== I/O Bound Tasks ===")
    tpe.run_io_bound_tasks(5)
    
    print("\n=== Tasks with Timeouts ===")
    tpe.run_tasks_with_timeouts()
    
    tpe.shutdown()
