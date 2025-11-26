import os
import threading
import multiprocessing
import time
import random
from threading import Thread, Lock, RLock, Semaphore, BoundedSemaphore, Condition
from utils import IsWinOS

class LockExamples:
    def __init__(self):
        self.counter = 0
        self.lock = Lock()
        self.rlock = RLock()
        self.semaphore = Semaphore(3) ## 3 Threads Simultaneamente
        self.bounded_semaphore = BoundedSemaphore(2)
        self.condition = Condition()
        self.shared_list = []

    def unsafe_increment(self):
        """Incremento sem lock - pode causar race condition"""
        temp = self.counter
        time.sleep(0.001) ## Simulação de Atraso
        self.counter = temp + 1

    def safe_increment_with_lock(self):
        """Incremento com lock - thread-safe"""
        with self.lock:
            temp = self.counter
            time.sleep(0.001)
            self.counter = temp + 1

    def demonstrate_race_condition(self, num_threads=10):
        """Demonstra race condition"""
        print("=== Demonstrating Race Condition ===")
        self.counter = 0
        threads = []
        
        for i in range(num_threads):
            thread = Thread(target=self.unsafe_increment)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        print(f"Expected: {num_threads}, Got: {self.counter}")

    def demonstrate_thread_safe(self, num_threads=10):
        """Demonstra operação thread-safe"""
        print("=== Demonstrating Thread Safety with Lock ===")
        self.counter = 0
        threads = []
        
        for i in range(num_threads):
            thread = Thread(target=self.safe_increment_with_lock)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        print(f"Expected: {num_threads}, Got: {self.counter}")

    def worker_with_semaphore(self, worker_id):
        """Worker usando semáforo para limitar concorrência"""
        with self.semaphore:
            print(f"[{threading.current_thread().name}] Worker {worker_id} acquired semaphore")
            time.sleep(1)
            print(f"[{threading.current_thread().name}] Worker {worker_id} releasing semaphore")

    def demonstrate_semaphore(self, num_workers=6):
        """Demonstra uso de semáforo"""
        print("=== Demonstrating Semaphore (max 3 concurrent) ===")
        threads = []
        
        for i in range(num_workers):
            thread = Thread(target=self.worker_with_semaphore, args=(i+1,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()

    def producer_with_condition(self):
        """Produtor usando condition variable"""
        with self.condition:
            print("Producer: Producing item...")
            time.sleep(1)
            self.shared_list.append("new_item")
            print("Producer: Notifying consumers...")
            self.condition.notify_all()

    def consumer_with_condition(self, consumer_id):
        """Consumidor usando condition variable"""
        with self.condition:
            print(f"Consumer {consumer_id}: Waiting for item...")
            while len(self.shared_list) == 0:
                self.condition.wait()
            item = self.shared_list.pop()
            print(f"Consumer {consumer_id}: Consumed {item}")

    def demonstrate_condition_variable(self):
        """Demonstra condition variables"""
        print("=== Demonstrating Condition Variables ===")
        
        consumer1 = Thread(target=self.consumer_with_condition, args=(1,))
        consumer2 = Thread(target=self.consumer_with_condition, args=(2,))
        
        consumer1.start()
        consumer2.start()
        
        time.sleep(0.5) 
        
        producer = Thread(target=self.producer_with_condition)
        producer.start()
        
        consumer1.join()
        consumer2.join()
        producer.join()

class MultiprocessingSync:
    def __init__(self):
        self.manager = multiprocessing.Manager()
        self.shared_counter = self.manager.Value('i', 0)
        self.shared_list = self.manager.list()
        self.lock = self.manager.Lock()

    def safe_increment_mp(self, process_id):
        """Incremento seguro com multiprocessing"""
        with self.lock:
            current = self.shared_counter.value
            time.sleep(0.01)
            self.shared_counter.value = current + 1
            print(f"[PID {os.getpid()}] Process {process_id} incremented counter to {self.shared_counter.value}")

    def demonstrate_mp_sync(self, num_processes=5):
        """Demonstra sincronização com multiprocessing"""
        print("=== Multiprocessing Synchronization ===")
        
        processes = []
        for i in range(num_processes):
            p = multiprocessing.Process(target=self.safe_increment_mp, args=(i+1,))
            processes.append(p)
            p.start()
        
        for p in processes:
            p.join()
        
        print(f"Final counter value: {self.shared_counter.value}")

if __name__ == "__main__":
    """ Verificar se o Sistema Operativo é Windows """
    IsWinOS()

    le = LockExamples()
    
    le.demonstrate_race_condition(10)
    le.demonstrate_thread_safe(10)
    le.demonstrate_semaphore(6)
    le.demonstrate_condition_variable()
    
    print("\n" + "="*50)
    mps = MultiprocessingSync()
    mps.demonstrate_mp_sync(5)