import os
import threading
from threading import Thread, current_thread
import multiprocessing
from multiprocessing import Process, current_process, cpu_count
import queue
from queue import Empty
import time
import random
from utils import IsWinOS

class ThreadingQueue:
    def __init__(self, max_size=10):
        self.queue = queue.Queue(maxsize=max_size)
        self.producer_threads = []
        self.consumer_threads = []
        self.running = False

    def producer(self, producer_id, items_to_produce):
        """Producer thread that adds items to the queue"""
        for i in range(items_to_produce):
            item = f"Item-{producer_id}-{i}"
            self.queue.put(item)
            print(f"[{current_thread().name}] Produced: {item}")
            time.sleep(random.uniform(0.1, 0.3))
        
        print(f"[{current_thread().name}] Producer {producer_id} finished")

    def consumer(self, consumer_id):
        """Consumer thread that removes items from the queue"""
        while self.running or not self.queue.empty():
            try:
                item = self.queue.get(timeout=1)
                print(f"[{current_thread().name}] Consumed: {item}")
                self.queue.task_done()
                time.sleep(random.uniform(0.1, 0.3))
            except Empty:
                continue
        
        print(f"[{current_thread().name}] Consumer {consumer_id} finished")

    def start_producer_consumer(self, num_producers=2, num_consumers=3, items_per_producer=5):
        """Starts the producer-consumer system"""
        self.running = True
        
        for i in range(num_producers):
            thread = Thread(
                target=self.producer,
                args=(i+1, items_per_producer),
                name=f"Producer-{i+1}"
            )
            self.producer_threads.append(thread)
            thread.start()

        for i in range(num_consumers):
            thread = Thread(
                target=self.consumer,
                args=(i+1,),
                name=f"Consumer-{i+1}"
            )
            self.consumer_threads.append(thread)
            thread.start()

        for thread in self.producer_threads:
            thread.join()

        self.running = False

        for thread in self.consumer_threads:
            thread.join()

        print("All producer-consumer threads finished")

    def worker_with_results(self, input_queue, output_queue, worker_id):
        """Worker that processes items and returns results"""
        while True:
            try:
                item = input_queue.get(timeout=1)
                if item is None:
                    input_queue.task_done()
                    break
                
                result = f"Processed({item}) by worker {worker_id}"
                output_queue.put(result)
                print(f"[{current_thread().name}] Processed: {item} -> {result}")
                input_queue.task_done()
                time.sleep(random.uniform(0.1, 0.3))
                
            except Empty:
                continue
        
        print(f"[{current_thread().name}] Worker {worker_id} finished")

    def start_workflow_system(self, num_workers=3, items_to_process=10):
        """Workflow processing system"""
        input_queue = queue.Queue()
        output_queue = queue.Queue()
        workers = []

        for i in range(items_to_process):
            input_queue.put(f"Task-{i+1}")

        for i in range(num_workers):
            thread = Thread(
                target=self.worker_with_results,
                args=(input_queue, output_queue, i+1),
                name=f"Worker-{i+1}"
            )
            workers.append(thread)
            thread.start()

        for _ in range(num_workers):
            input_queue.put(None)

        for thread in workers:
            thread.join()

        results = []
        while not output_queue.empty():
            results.append(output_queue.get())

        print(f"Workflow finished. Results: {results}")
        return results

class MultiprocessingQueue:
    def __init__(self, max_size=10):
        self.queue = multiprocessing.Queue(maxsize=max_size)
        self.processes = []
        self.max_processes = cpu_count()

    def producer(self, producer_id, items_to_produce, queue):
        """Producer process"""
        for i in range(items_to_produce):
            item = f"MP-Item-{producer_id}-{i}"
            queue.put(item)
            print(f"[PID {os.getpid()}] Produced: {item}")
            time.sleep(random.uniform(0.1, 0.3))
        
        print(f"[PID {os.getpid()}] Producer {producer_id} finished")

    def consumer(self, consumer_id, queue):
        """Consumer process"""
        while True:
            item = queue.get()
            if item is None:
                break
            
            print(f"[PID {os.getpid()}] Consumed: {item}")
            time.sleep(random.uniform(0.2, 0.6))
        
        print(f"[PID {os.getpid()}] Consumer {consumer_id} finished")

    def start_mp_producer_consumer(self, num_producers=2, num_consumers=3, items_per_producer=5):
        """Multiprocessing producer-consumer system"""
        processes = []

        for i in range(num_consumers):
            p = Process(target=self.consumer, args=(i+1, self.queue))
            processes.append(p)
            p.start()

        for i in range(num_producers):
            p = Process(target=self.producer, args=(i+1, items_per_producer, self.queue))
            processes.append(p)
            p.start()

        for p in processes[:num_producers]:
            p.join()

        for _ in range(num_consumers):
            self.queue.put(None)

        for p in processes[num_producers:]:
            p.join()

        print("All multiprocessing producer-consumer processes finished")

    def cpu_worker(self, input_queue, output_queue, worker_id):
        """CPU-intensive task worker"""
        while True:
            try:
                task = input_queue.get(timeout=1)
                if task is None:
                    break
                
                # Simulate CPU-bound processing
                result = sum(i*i for i in range(task))
                output_queue.put((worker_id, task, result))
                print(f"[PID {os.getpid()}] Worker {worker_id} processed task {task}")
                
            except:
                continue
        
        print(f"[PID {os.getpid()}] CPU Worker {worker_id} finished")

    def start_cpu_intensive_workflow(self, num_workers=None, tasks=None):
        """System for CPU-intensive tasks"""
        if num_workers is None:
            num_workers = self.max_processes
        if tasks is None:
            tasks = [100000, 200000, 300000, 400000, 500000]

        start_time = time.time()

        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        processes = []

        for task in tasks:
            input_queue.put(task)

        for i in range(num_workers):
            p = Process(target=self.cpu_worker, args=(input_queue, output_queue, i+1))
            processes.append(p)
            p.start()

        for _ in range(num_workers):
            input_queue.put(None)

        results = []
        for _ in range(len(tasks)):
            result = output_queue.get()
            results.append(result)

        for p in processes:
            p.join()

        end_time = time.time() - start_time

        print(f"CPU-intensive workflow finished. Results: {results} - Time elapsed: {end_time:.2f} seconds")
        return results

if __name__ == "__main__":
    """Check if the Operating System is Windows"""
    IsWinOS()

    """ Threading Queue Example """
    tq = ThreadingQueue(max_size=5)
    
    print("=== Producer-Consumer Example ===")
    tq.start_producer_consumer(num_producers=2, num_consumers=3, items_per_producer=5)
    
    print("\n=== Workflow System Example ===")
    results = tq.start_workflow_system(num_workers=3, items_to_process=8)

    """ Multiprocessing Queue Example """
    mpq = MultiprocessingQueue()
    
    print("=== Multiprocessing Producer-Consumer ===")
    mpq.start_mp_producer_consumer(num_producers=2, num_consumers=2, items_per_producer=4)
    
    print("\n=== CPU-Intensive Workflow ===")
    results = mpq.start_cpu_intensive_workflow(num_workers=3, tasks=[100000, 200000, 150000])