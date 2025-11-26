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
        """Tarefa simples para demonstração"""
        print(f"[{threading.current_thread().name}] Task {task_id} started, sleeping {duration}s")
        time.sleep(duration)
        result = f"Task {task_id} completed after {duration}s"
        print(f"[{threading.current_thread().name}] {result}")
        return result

    def run_simple_tasks(self, num_tasks=8):
        """Executa múltiplas tarefas simples"""
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
        """Tarefa que simula processamento CPU (não ideal para threading)"""
        return sum(i * i for i in range(number))

    def io_bound_task(self, task_id):
        """Tarefa que simula I/O (ideal para threading)"""
        print(f"[{threading.current_thread().name}] IO Task {task_id} started")
        time.sleep(random.uniform(0.5, 1.5)) ## Simuação Input/Output (I/O)
        result = f"IO Task {task_id} completed"
        print(f"[{threading.current_thread().name}] {result}")
        return result

    def run_io_bound_tasks(self, num_tasks=10):
        """Executa tarefas I/O bound"""
        print(f"Running {num_tasks} I/O bound tasks...")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.io_bound_task, i+1) for i in range(num_tasks)]
            
            results = []
            for future in as_completed(futures):
                results.append(future.result())
        
        return results

    def process_with_timeout(self, task_id, duration, timeout=1.0):
        """Tarefa com timeout"""
        try:
            future = self.executor.submit(self.simple_task, task_id, duration)
            result = future.result(timeout=timeout)
            return f"SUCCESS: {result}"
        except concurrent.futures.TimeoutError:
            return f"TIMEOUT: Task {task_id} exceeded {timeout}s"

    def run_tasks_with_timeouts(self):
        """Executa tarefas com timeouts"""
        tasks = [
            ## Formato do Objeto:
            ## task_id, duration, timeout
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
        """Encerra o executor"""
        self.executor.shutdown(wait=True)
        print("ThreadPoolExecutor shutdown")

if __name__ == "__main__":
    """ Verificar se o Sistema Operativo é Windows """
    IsWinOS()

    tpe = ThreadPoolExample(max_workers=3)
    
    print("=== Simple Thread Pool Tasks ===")
    tpe.run_simple_tasks(6)
    
    print("\n=== I/O Bound Tasks ===")
    tpe.run_io_bound_tasks(5)
    
    print("\n=== Tasks with Timeouts ===")
    tpe.run_tasks_with_timeouts()
    
    tpe.shutdown()