import os
import threading
import multiprocessing
import time
import random
from threading import Thread, Lock, RLock, Semaphore, BoundedSemaphore, Condition
from multiprocessing import Manager
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import csv
import psutil
from utils import IsWinOS

def get_memory_usage():
    """Get current process memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

class ThreadingLogger:
    """Logger class for storing threading results in CSV files"""
    def __init__(self, results_file="data/locks_results.csv", 
                 summary_file="data/locks_summary.csv"):
        self.results_file = results_file
        self.summary_file = summary_file
        self.results = []
        self.summary_data = []
        
        os.makedirs("data", exist_ok=True)
        os.makedirs("plots", exist_ok=True)
        
        self.fieldnames = [
            'timestamp', 'test_type', 'num_threads', 'start_time', 
            'end_time', 'elapsed_time', 'memory_usage_mb', 'cpu_count',
            'system_load', 'test_run', 'expected_value', 'actual_value', 
            'success', 'lock_type', 'concurrent_limit'
        ]
    
    def write_result_to_csv(self, result_dict):
        """Write a single result to CSV file"""
        file_exists = os.path.isfile(self.results_file)
        
        with open(self.results_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            
            if not file_exists or os.path.getsize(self.results_file) == 0:
                writer.writeheader()
            
            # Garantir que todas as colunas estejam presentes
            row_to_write = {field: result_dict.get(field, '') for field in self.fieldnames}
            writer.writerow(row_to_write)
    
    def log_result(self, test_type, num_threads, start_time, end_time, 
                   expected_value, actual_value, lock_type=None, 
                   concurrent_limit=None, test_run=1):
        """Log a single test result and append to CSV file"""
        elapsed_time = end_time - start_time
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        memory_usage = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        success = expected_value == actual_value
        
        # Create result dictionary
        result = {
            'timestamp': timestamp,
            'test_type': test_type,
            'num_threads': num_threads,
            'start_time': start_time,
            'end_time': end_time,
            'elapsed_time': round(elapsed_time, 4),  # 4 casas decimais
            'memory_usage_mb': round(memory_usage, 4),  # 4 casas decimais
            'cpu_count': os.cpu_count(),
            'system_load': round(psutil.cpu_percent(interval=0.1), 4),  # 4 casas
            'test_run': test_run,
            'expected_value': expected_value,
            'actual_value': actual_value,
            'success': success,
            'lock_type': lock_type or 'none',
            'concurrent_limit': concurrent_limit or 0
        }
        
        self.results.append(result)
        
        # Write to CSV
        self.write_result_to_csv(result)
        
        return result
    
    def calculate_summary(self, test_run=None):
        """Calculate summary statistics and update summary CSV"""
        if not self.results:
            return
        
        # Group and process test data using Pandas for statistical analysis
        df = pd.DataFrame(self.results)
        
        if test_run is not None:
            df = df[df['test_run'] == test_run]
        
        if len(df) == 0:
            return
        
        summary = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_tests': len(df),
            'success_rate': round(df['success'].mean() * 100, 2),
            'avg_elapsed_time': round(df['elapsed_time'].mean(), 4),
            'min_elapsed_time': round(df['elapsed_time'].min(), 4),
            'max_elapsed_time': round(df['elapsed_time'].max(), 4),
            'std_elapsed_time': round(df['elapsed_time'].std(), 4),
            'avg_memory_usage': round(df['memory_usage_mb'].mean(), 4),
            'avg_system_load': round(df['system_load'].mean(), 4),
            'test_run': test_run if test_run else 'all'
        }
        
        self.summary_data.append(summary)
        
        # Append current summary snippet to CSV, maintaining potential visual separators
        df_summary = pd.DataFrame([summary])
        file_exists = os.path.isfile(self.summary_file) and os.path.getsize(self.summary_file) > 0
        df_summary.round(4).to_csv(self.summary_file, mode='a', index=False, header=not file_exists)
        
        return summary

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

def demonstrate_race_condition(num_threads=10, logger=None, test_run=1):
    """Demonstrate race condition with logging"""
    print("=== Demonstrating Race Condition ===")
    start_time = time.time()
    
    counter = [0]
    threads = []
    
    def worker():
        temp = counter[0]
        time.sleep(0.001)
        counter[0] = temp + 1
    
    for _ in range(num_threads):
        thread = Thread(target=worker)
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    
    final_counter = counter[0]
    result = f"Expected: {num_threads}, Got: {final_counter}"
    print(result)
    
    if logger:
        logger.log_result(
            test_type="race_condition",
            num_threads=num_threads,
            start_time=start_time,
            end_time=end_time,
            expected_value=num_threads,
            actual_value=final_counter,
            lock_type="none",
            test_run=test_run
        )
    
    return final_counter

def demonstrate_thread_safe(num_threads=10, logger=None, test_run=1):
    """Demonstrate thread-safe operation with lock with logging"""
    print("=== Demonstrating Thread Safety with Lock ===")
    start_time = time.time()
    
    counter = [0]
    lock = Lock()
    threads = []
    
    def worker():
        with lock:
            temp = counter[0]
            time.sleep(0.001)
            counter[0] = temp + 1
    
    for _ in range(num_threads):
        thread = Thread(target=worker)
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    
    final_counter = counter[0]
    result = f"Expected: {num_threads}, Got: {final_counter}"
    print(result)
    
    if logger:
        logger.log_result(
            test_type="thread_safe_lock",
            num_threads=num_threads,
            start_time=start_time,
            end_time=end_time,
            expected_value=num_threads,
            actual_value=final_counter,
            lock_type="Lock",
            test_run=test_run
        )
    
    return final_counter

def worker_with_semaphore(worker_id, semaphore, results_list, lock):
    """Worker using semaphore to limit concurrency"""
    with semaphore:
        thread_start = time.time()
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Worker {worker_id} acquired semaphore")
        time.sleep(1)
        print(f"[{thread_name}] Worker {worker_id} releasing semaphore")
        thread_end = time.time()
        
        with lock:
            results_list.append({
                'worker_id': worker_id,
                'thread_name': thread_name,
                'start_time': thread_start,
                'end_time': thread_end,
                'elapsed_time': thread_end - thread_start
            })

def demonstrate_semaphore(num_workers=6, logger=None, test_run=1):
    """Demonstrate the use of a semaphore with logging"""
    print("=== Demonstrating Semaphore (max 3 concurrent) ===")
    start_time = time.time()
    semaphore = Semaphore(3)
    results_list = []
    results_lock = Lock()
    threads = []
    
    for i in range(num_workers):
        thread = Thread(target=worker_with_semaphore, 
                       args=(i + 1, semaphore, results_list, results_lock))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    
    # Log results if logger is provided
    if logger:
        avg_elapsed = np.mean([r['elapsed_time'] for r in results_list]) if results_list else 0
        logger.log_result(
            test_type="semaphore",
            num_threads=num_workers,
            start_time=start_time,
            end_time=end_time,
            expected_value=num_workers,
            actual_value=len(results_list),
            lock_type="Semaphore",
            concurrent_limit=3,
            test_run=test_run
        )
    
    return len(results_list)

def consumer_with_condition(shared_list, condition, consumer_id, results_list, lock):
    """Consumer using condition variable"""
    with condition:
        print(f"Consumer {consumer_id}: Waiting for item...")
        thread_start = time.time()
        
        # Verificar se já há item disponível
        if len(shared_list) == 0:
            print(f"Consumer {consumer_id}: No item available, waiting...")
            condition.wait(timeout=5.0)  # Timeout de 5 segundos
        
        success = False
        if len(shared_list) > 0:
            item = shared_list.pop()  # Consume the item
            print(f"Consumer {consumer_id}: Consumed {item}")
            success = True
        else:
            print(f"Consumer {consumer_id}: No item to consume, exiting...")
        
        thread_end = time.time()
        
        with lock:
            results_list.append({
                'consumer_id': consumer_id,
                'start_time': thread_start,
                'end_time': thread_end,
                'elapsed_time': thread_end - thread_start,
                'success': success
            })
    
    return success

def producer_with_condition(shared_list, condition, num_items=2, results_list=None, lock=None):
    """Producer using condition variable"""
    with condition:
        print("Producer: Producing item...")
        thread_start = time.time()
        
        # Produzir múltiplos itens para múltiplos consumidores
        for i in range(num_items):
            time.sleep(0.5)  ## Simulate producing an item
            item = f"new_item_{i+1}"
            shared_list.append(item)
            print(f"Producer: Produced {item}")
        
        print(f"Producer: Notifying consumers... ({num_items} items available)")
        condition.notify_all()  ## Notify all waiting threads
        
        thread_end = time.time()
        
        if results_list is not None and lock is not None:
            with lock:
                results_list.append({
                    'producer_id': 1,
                    'start_time': thread_start,
                    'end_time': thread_end,
                    'elapsed_time': thread_end - thread_start,
                    'items_produced': num_items
                })

def demonstrate_condition_variable(logger=None, test_run=1):
    """Demonstrate condition variables with logging"""
    print("=== Demonstrating Condition Variables ===")
    
    start_time = time.time()
    shared_list = []
    condition = threading.Condition()
    results_list = []
    results_lock = Lock()
    
    num_items_needed = 2
    
    consumer1 = threading.Thread(
        target=consumer_with_condition, 
        args=(shared_list, condition, 1, results_list, results_lock),
        name="Consumer-1"
    )
    consumer2 = threading.Thread(
        target=consumer_with_condition, 
        args=(shared_list, condition, 2, results_list, results_lock),
        name="Consumer-2"
    )
    
    consumer1.start()
    consumer2.start()
    
    time.sleep(0.5)
    
    producer = threading.Thread(
        target=producer_with_condition, 
        args=(shared_list, condition, num_items_needed, results_list, results_lock),
        name="Producer"
    )
    producer.start()
    
    consumer1.join(timeout=10.0)
    consumer2.join(timeout=10.0)
    producer.join(timeout=10.0)
    
    # Verificar se as threads ainda estão vivas
    if consumer1.is_alive():
        print("Warning: Consumer 1 thread still alive after timeout!")
    if consumer2.is_alive():
        print("Warning: Consumer 2 thread still alive after timeout!")
    if producer.is_alive():
        print("Warning: Producer thread still alive after timeout!")
    
    end_time = time.time()
    
    # Contar quantos consumidores tiveram sucesso
    successful_consumers = sum(1 for r in results_list if r.get('success', False))
    
    # Log results if logger is provided
    if logger:
        logger.log_result(
            test_type="condition_variable",
            num_threads=3,  # 2 consumers + 1 producer
            start_time=start_time,
            end_time=end_time,
            expected_value=num_items_needed,  # Esperamos 2 itens consumidos
            actual_value=successful_consumers,  # 2 consumidores bem-sucedidos
            lock_type="Condition",
            test_run=test_run
        )
    
    return len(results_list)

def safe_increment_mp(shared_counter, lock, process_id, results_list, results_lock):
    """Safe increment with multiprocessing"""
    with lock:
        current = shared_counter.value
        time.sleep(0.01)
        shared_counter.value = current + 1
        thread_end = time.time()
        print(f"[PID {os.getpid()}] Process {process_id} incremented counter to {shared_counter.value}")
        
        with results_lock:
            results_list.append({
                'process_id': process_id,
                'pid': os.getpid(),
                'counter_value': shared_counter.value,
                'timestamp': thread_end
            })

def demonstrate_mp_sync(num_processes=5, logger=None, test_run=1):
    """Demonstrate synchronization with multiprocessing with logging"""
    print("=== Multiprocessing Synchronization ===")
    
    start_time = time.time()
    manager = Manager()
    shared_counter = manager.Value('i', 0)
    lock = manager.Lock()
    results_list = manager.list()
    results_lock = manager.Lock()
    
    processes = []
    for i in range(num_processes):
        p = multiprocessing.Process(
            target=safe_increment_mp, 
            args=(shared_counter, lock, i+1, results_list, results_lock)
        )
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join()
    
    end_time = time.time()
    
    final_result = f"Final counter value: {shared_counter.value}"
    print(final_result)
    
    # Log results if logger is provided
    if logger:
        logger.log_result(
            test_type="multiprocessing_sync",
            num_threads=num_processes,
            start_time=start_time,
            end_time=end_time,
            expected_value=num_processes,
            actual_value=shared_counter.value,
            lock_type="Manager.Lock",
            test_run=test_run
        )
    
    return shared_counter.value

def read_csv_safe(file_path):
    """Lê um arquivo CSV de forma segura, lidando com possíveis inconsistências"""
    try:
        # Load measurement data using Pandas for data cleansing and normalization
        df = pd.read_csv(file_path)
        
        # Identify numerical columns for precision rounding
        for col in df.select_dtypes(include=[np.number]).columns:
            if col not in ['num_threads', 'test_run', 'expected_value', 'actual_value', 
                          'success', 'concurrent_limit', 'cpu_count']:
                df[col] = df[col].round(4)
        
        return df
    except pd.errors.ParserError as e:
        print(f"Warning: Parser error reading {file_path}: {e}")
        return pd.DataFrame()
    except FileNotFoundError:
        print(f"File {file_path} not found!")
        return pd.DataFrame()
    except Exception as e:
        print(f"Unexpected error reading {file_path}: {e}")
        return pd.DataFrame()

def create_threading_plots():
    """Create a standardized 2x3 visualization grid for synchronization analysis"""
    results_file = "data/locks_results.csv"
    summary_file = "data/locks_summary.csv"
    
    if not os.path.exists(results_file):
        print(f"Error: {results_file} not found!")
        return
    
    df = read_csv_safe(results_file)
    if df.empty:
        print("No data available for plotting.")
        return

    plt.figure(figsize=(18, 12))
    
    def format_two_decimals(x, pos):
        return f'{x:.2f}'

    # 1. Average Time per Test Type (Proxy for Thread Index in this context)
    plt.subplot(2, 3, 1)
    if 'test_type' in df.columns and 'elapsed_time' in df.columns:
        try:
            avg_time_type = df.groupby('test_type')['elapsed_time'].mean()
            bars = avg_time_type.plot(kind='bar', color='skyblue', edgecolor='black', alpha=0.8)
            plt.title('Average Time per Test Type', fontsize=12, fontweight='bold')
            plt.ylabel('Time (seconds)')
            plt.xticks(rotation=45, ha='right')
            plt.grid(True, alpha=0.3, axis='y')
            for bar in bars.patches:
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), 
                        f'{bar.get_height():.2f}s', ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error Plot 1: {e}")

    # 2. Average Execution Time per Test Run
    plt.subplot(2, 3, 2)
    if 'test_run' in df.columns and 'elapsed_time' in df.columns:
        try:
            trends = df.groupby('test_run')['elapsed_time'].mean()
            plt.plot(trends.index, trends.values, marker='o', color='blue', linewidth=2)
            plt.title('Average Execution Time per Test Run', fontsize=12, fontweight='bold')
            plt.xlabel('Test Run')
            plt.ylabel('Time (seconds)')
            plt.grid(True, alpha=0.3)
            for x, y in zip(trends.index, trends.values):
                plt.text(x, y, f'{y:.2f}s', ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error Plot 2: {e}")

    # 3. Memory Usage Distribution
    plt.subplot(2, 3, 3)
    if 'memory_usage_mb' in df.columns:
        try:
            plt.hist(df['memory_usage_mb'].dropna(), bins=15, color='salmon', alpha=0.7, edgecolor='black')
            plt.title('Memory Usage Distribution', fontsize=12, fontweight='bold')
            plt.xlabel('Memory (MB)')
            plt.ylabel('Frequency')
            plt.grid(True, alpha=0.3, axis='y')
        except Exception as e:
            print(f"Error Plot 3: {e}")

    # 4. Average System Load per Test Run
    plt.subplot(2, 3, 4)
    if 'test_run' in df.columns and 'system_load' in df.columns:
        try:
            load_trends = df.groupby('test_run')['system_load'].mean()
            bars = load_trends.plot(kind='bar', color='orange', alpha=0.7, edgecolor='black')
            plt.title('Average System Load per Test Run', fontsize=12, fontweight='bold')
            plt.ylabel('System Load (%)')
            plt.xticks(rotation=0)
            plt.grid(True, alpha=0.3, axis='y')
            for bar in bars.patches:
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), 
                        f'{bar.get_height():.1f}%', ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error Plot 4: {e}")

    # 5. Number of Threads per Test Run
    plt.subplot(2, 3, 5)
    if 'test_run' in df.columns and 'num_threads' in df.columns:
        try:
            thread_counts = df.groupby('test_run')['num_threads'].mean()
            bars = thread_counts.plot(kind='bar', color='purple', alpha=0.7, edgecolor='black')
            plt.title('Number of Threads per Test Run', fontsize=12, fontweight='bold')
            plt.ylabel('Thread Count')
            plt.grid(True, alpha=0.3, axis='y')
            for bar in bars.patches:
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), 
                        f'{int(bar.get_height())}', ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error Plot 5: {e}")

    # 6. Total Execution Time per Test Run
    plt.subplot(2, 3, 6)
    if 'test_run' in df.columns and 'elapsed_time' in df.columns:
        try:
            total_times = df.groupby('test_run')['elapsed_time'].sum()
            plt.fill_between(total_times.index, total_times.values, color='red', alpha=0.4)
            plt.plot(total_times.index, total_times.values, color='red', marker='s', linewidth=2)
            plt.title('Total Execution Time per Test Run', fontsize=12, fontweight='bold')
            plt.ylabel('Total Time (seconds)')
            plt.grid(True, alpha=0.3)
            for x, y in zip(total_times.index, total_times.values):
                plt.text(x, y, f'{y:.2f}s', ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error Plot 6: {e}")

    plt.tight_layout()
    plt.savefig("plots/locks_analysis_plots.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    if os.path.exists(summary_file):
        try:
            df_summary = read_csv_safe(summary_file)
            if not df_summary.empty:
                print(f"\nThreading Summary Statistics:")
                print(df_summary.tail(min(5, len(df_summary))))
        except Exception as e:
            print(f"Error reading summary file: {e}")


def cleanup_old_data():
    """Insere um separador nos dados existentes em vez de limpar"""
    results_file = "data/locks_results.csv"
    summary_file = "data/locks_summary.csv"
    
    for file_path in [results_file, summary_file]:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            try:
                with open(file_path, 'a') as f:
                    f.write('\n')
                print(f"✓ Added separator to {file_path}")
            except Exception as e:
                print(f"Error appending separator to {file_path}: {e}")

def main():
    """Check if the Operating System is Windows"""
    IsWinOS()
    
    cleanup_old_data()
    
    logger = ThreadingLogger()
    
    print("\n" + "="*60)
    print("THREADING AND MULTIPROCESSING SYNCHRONIZATION TESTS")
    print("="*60)
    
    # Configurações de teste
    test_configs = [
        ("Race Condition", 10),
        ("Thread Safe", 10),
        ("Semaphore", 6),
        ("Condition Variable", None),  # Número fixo de threads
        ("Multiprocessing Sync", 5)
    ]
    
    all_results = []
    
    for test_run in range(1, 4):
        print(f"\n{'#'*60}")
        print(f"STARTING TEST RUN {test_run} OF 3")
        print(f"{'#'*60}")
        
        run_results = []
        
        for test_name, num_threads in test_configs:
            print(f"\n{'='*40}")
            print(f"Test Run {test_run}: {test_name}")
            print(f"{'='*40}")
            
            start_time = time.time()
            
            if test_name == "Race Condition":
                result = demonstrate_race_condition(num_threads, logger, test_run)
            elif test_name == "Thread Safe":
                result = demonstrate_thread_safe(num_threads, logger, test_run)
            elif test_name == "Semaphore":
                result = demonstrate_semaphore(num_threads, logger, test_run)
            elif test_name == "Condition Variable":
                result = demonstrate_condition_variable(logger, test_run)
            elif test_name == "Multiprocessing Sync":
                result = demonstrate_mp_sync(num_threads, logger, test_run)
            
            end_time = time.time()
            
            run_results.append({
                'test_name': test_name,
                'num_threads': num_threads or 3,
                'elapsed_time': round(end_time - start_time, 4),
                'result': result
            })
            
            time.sleep(1)  # Pequena pausa entre testes
        
        all_results.append(run_results)
        
        # Calcular summary para este test_run
        logger.calculate_summary(test_run)
        
        print(f"\nTest Run {test_run} Results:")
        for res in run_results:
            print(f"  {res['test_name']}: {res['result']} in {res['elapsed_time']:.4f}s")
    
    # Calcular summary final
    print(f"\n{'#'*60}")
    print("CALCULATING FINAL SUMMARY (AVERAGE OF 3 RUNS)")
    print(f"{'#'*60}")
    
    final_summary = logger.calculate_summary()
    
    if final_summary:
        print(f"\nFinal Summary Statistics (Average of 3 test runs):")
        for key, value in final_summary.items():
            if key != 'timestamp':
                print(f"  {key}: {value}")
    
    print(f"\n{'#'*60}")
    print("GENERATING ANALYSIS PLOTS")
    print(f"{'#'*60}")
    
    create_threading_plots()
    
    print(f"\n{'#'*60}")
    print("CONSOLIDATED RESULTS (3 RUNS)")
    print(f"{'#'*60}")
    
    results_by_test = {}
    for run in all_results:
        for test in run:
            test_name = test['test_name']
            if test_name not in results_by_test:
                results_by_test[test_name] = []
            results_by_test[test_name].append(test['elapsed_time'])
    
    for test_name, times in results_by_test.items():
        avg_time = np.mean(times)
        std_time = np.std(times)
        min_time = np.min(times)
        max_time = np.max(times)
        
        print(f"\n{test_name}:")
        print(f"  Average: {avg_time:.4f} ± {std_time:.4f} seconds")
        print(f"  Range: {min_time:.4f} - {max_time:.4f} seconds")
        print(f"  Variation: {((max_time-min_time)/avg_time*100):.2f}%")
    
    print(f"\n{'='*60}")
    print("ALL TESTS COMPLETED SUCCESSFULLY")
    print(f"Data saved to: /data/locks_results.csv")
    print(f"Summary saved to: /data/locks_summary.csv")
    print(f"Plots saved to: /plots/locks_analysis_plots.png")
    print(f"{'='*60}")
    
    return True

if __name__ == "__main__":
    """Check if the Operating System is Windows"""
    IsWinOS()

    # Execute main function
    main()