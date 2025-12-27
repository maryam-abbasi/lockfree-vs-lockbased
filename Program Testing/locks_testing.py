import os
import threading
import multiprocessing
import time
import random
import unittest
from threading import Thread, Lock, RLock, Semaphore, BoundedSemaphore, Condition
from multiprocessing import Manager

from utils import IsWinOS

def unsafe_increment(counter):
    """Unsafe increment without lock - may cause race condition"""
    temp = counter
    time.sleep(0.001)  ## Simulating delay
    counter = temp + 1
    return counter

def safe_increment_with_lock(counter, lock):
    """Safe increment with lock - thread-safe"""
    with lock:
        temp = counter
        time.sleep(0.001)
        counter = temp + 1
    return counter

def demonstrate_race_condition(num_threads=10):
    """Demonstrate race condition"""
    print("=== Demonstrating Race Condition ===")
    counter = 0
    threads = []
    
    def worker():
        nonlocal counter
        counter = unsafe_increment(counter)
    
    for _ in range(num_threads):
        thread = Thread(target=worker)
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    print(f"Expected: {num_threads}, Got: {counter}")

def demonstrate_thread_safe(num_threads=10):
    """Demonstrate thread-safe operation with lock"""
    print("=== Demonstrating Thread Safety with Lock ===")
    counter = 0
    lock = Lock()
    threads = []
    
    def worker():
        nonlocal counter
        counter = safe_increment_with_lock(counter, lock)
    
    for _ in range(num_threads):
        thread = Thread(target=worker)
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    print(f"Expected: {num_threads}, Got: {counter}")

def worker_with_semaphore(worker_id, semaphore):
    """Worker using semaphore to limit concurrency"""
    with semaphore:
        print(f"[{threading.current_thread().name}] Worker {worker_id} acquired semaphore")
        time.sleep(1)
        print(f"[{threading.current_thread().name}] Worker {worker_id} releasing semaphore")

def demonstrate_semaphore(num_workers=6):
    """Demonstrate the use of a semaphore"""
    print("=== Demonstrating Semaphore (max 3 concurrent) ===")
    semaphore = Semaphore(3)
    threads = []
    
    for i in range(num_workers):
        thread = Thread(target=worker_with_semaphore, args=(i + 1, semaphore))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()

def consumer_with_condition(shared_list, condition, consumer_id):
    """Consumer using condition variable"""
    with condition:
        print(f"Consumer {consumer_id}: Waiting for item...")
        while len(shared_list) == 0:  ## Wait until the producer adds an item
            print(f"Consumer {consumer_id}: Still waiting for item...")
            condition.wait()  ## Wait for notification from producer
        
        if len(shared_list) > 0:
            item = shared_list.pop()  # Consume the item
            print(f"Consumer {consumer_id}: Consumed {item}")
        else:
            print(f"Consumer {consumer_id}: No item to consume, exiting...")

def producer_with_condition(shared_list, condition):
    """Producer using condition variable"""
    with condition:
        print("Producer: Producing item...")
        time.sleep(1)  ## Simulate producing an item
        shared_list.append("new_item")
        print("Producer: Notifying consumers...")
        condition.notify_all()  ## Notify all waiting threads

def demonstrate_condition_variable():
    """Demonstrate condition variables"""
    print("=== Demonstrating Condition Variables ===")
    
    shared_list = []
    condition = threading.Condition()
    
    consumer1 = threading.Thread(target=consumer_with_condition, args=(shared_list, condition, 1))
    consumer2 = threading.Thread(target=consumer_with_condition, args=(shared_list, condition, 2))
    
    consumer1.start()
    consumer2.start()
    
    time.sleep(0.5)  ## Simulate delay to ensure consumers are waiting
    
    producer = threading.Thread(target=producer_with_condition, args=(shared_list, condition))
    producer.start()
    
    consumer1.join()
    consumer2.join()
    producer.join()

def safe_increment_mp(shared_counter, lock):
    """Safe increment with multiprocessing"""
    with lock:
        current = shared_counter.value
        time.sleep(0.01)
        shared_counter.value = current + 1
        print(f"[PID {os.getpid()}] Process incremented counter to {shared_counter.value}")

def demonstrate_mp_sync(num_processes=5):
    """Demonstrate synchronization with multiprocessing"""
    print("=== Multiprocessing Synchronization ===")
    
    manager = Manager()
    shared_counter = manager.Value('i', 0)
    lock = manager.Lock()
    
    processes = []
    for i in range(num_processes):
        p = multiprocessing.Process(target=safe_increment_mp, args=(shared_counter, lock))
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join()
    
    print(f"Final counter value: {shared_counter.value}")

class TestSynchronizationMethods(unittest.TestCase):
    def test_race_condition(self):
        """Test race condition behavior"""
        # Expected counter value should be equal to the number of threads
        num_threads = 10
        with self.assertRaises(AssertionError):
            demonstrate_race_condition(num_threads)

    def test_thread_safe(self):
        """Test thread-safe increment with lock"""
        num_threads = 10
        demonstrate_thread_safe(num_threads)
        self.assertEqual(demonstrate_thread_safe(num_threads), num_threads)
    
    def test_semaphore(self):
        """Test semaphore behavior"""
        demonstrate_semaphore(6)
        # Semaphore limits concurrent access to a maximum of 3
    
    def test_condition_variable(self):
        """Test condition variable behavior"""
        demonstrate_condition_variable()
        # Ensure producer-consumer works in sync
    
    def test_multiprocessing_sync(self):
        """Test multiprocessing synchronization"""
        demonstrate_mp_sync(5)
        # Ensure counter value is updated correctly across multiple processes

if __name__ == "__main__":
    """Check if the Operating System is Windows"""
    IsWinOS()

    # Demonstrate different synchronization methods
    demonstrate_race_condition(10)
    demonstrate_thread_safe(10)
    demonstrate_semaphore(6)
    demonstrate_condition_variable()

    print("\n" + "=" * 50)
    demonstrate_mp_sync(5)
    
    # Run unit tests
    unittest.main()
