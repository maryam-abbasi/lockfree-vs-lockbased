import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import time
import random
import os
import csv
import psutil
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from utils import IsWinOS

def get_process_metrics():
    """Return metrics for the current process"""
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    system_load = psutil.cpu_percent(interval=None)
    return memory_mb, system_load

def read_csv_safe(file_path):
    """Safely read CSV file"""
    try:
        return pd.read_csv(file_path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return pd.DataFrame()

class ProcessPoolLogger:
    def __init__(self, results_file="data/processpool_results.csv", 
                 summary_file="data/processpool_summary.csv"):
        self.results_file = results_file
        self.summary_file = summary_file
        self.results = []
        self.fieldnames = ['timestamp', 'test_type', 'task_id', 'start_time', 'end_time', 
                          'elapsed_time', 'memory_usage_mb', 'system_load', 'test_run']
        
        self._initialize_csv()

    def _initialize_csv(self):
        os.makedirs(os.path.dirname(self.results_file), exist_ok=True)
        if not os.path.exists(self.results_file) or os.path.getsize(self.results_file) == 0:
            with open(self.results_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()

    def log_result(self, test_type, task_id, start_time, end_time, memory_mb, system_load, test_run=1):
        elapsed_time = end_time - start_time
        
        result_dict = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'test_type': test_type,
            'task_id': task_id,
            'start_time': start_time,
            'end_time': end_time,
            'elapsed_time': elapsed_time,
            'memory_usage_mb': memory_mb,
            'system_load': system_load,
            'test_run': test_run
        }
        
        self.results.append(result_dict)
        self.write_result_to_csv(result_dict)

    def write_result_to_csv(self, result_dict):
        with open(self.results_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writerow(result_dict)

    def calculate_summary(self, test_run=None):
        if not self.results:
            return None
        
        df = pd.DataFrame(self.results)
        if test_run:
            df = df[df['test_run'] == test_run]
            
        summary = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_tasks': len(df),
            'avg_elapsed_time': df['elapsed_time'].mean(),
            'min_elapsed_time': df['elapsed_time'].min(),
            'max_elapsed_time': df['elapsed_time'].max(),
            'avg_memory_usage': df['memory_usage_mb'].mean(),
            'avg_system_load': df['system_load'].mean(),
            'test_run': test_run
        }
        
        # Append summary to file
        os.makedirs(os.path.dirname(self.summary_file), exist_ok=True)
        file_exists = os.path.exists(self.summary_file) and os.path.getsize(self.summary_file) > 0
        
        with open(self.summary_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=summary.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(summary)
            
        return summary

class ProcessPoolExample:
    def __init__(self, logger=None, test_run=1):
        self.max_workers = multiprocessing.cpu_count()
        self.logger = logger
        self.test_run = test_run

    @staticmethod
    def cpu_intensive_task(number):
        """CPU-intensive task (ideal for multiprocessing)"""
        start_time = time.time()
        print(f"[PID {os.getpid()}] Processing number: {number}")
        result = sum(i * i for i in range(number))
        end_time = time.time()
        metrics = get_process_metrics()
        
        msg = f"PID {os.getpid()}: sum(i^2) for i<{number} = {result}"
        return msg, start_time, end_time, metrics

    def run_cpu_intensive_tasks(self, numbers):
        """Executes CPU-intensive tasks"""
        print(f"Running {len(numbers)} CPU-intensive tasks with {self.max_workers} workers...")

        start_time = time.time()
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # We must use static method or picklable function. Instance methods are tricky with ProcessPool
            futures = {executor.submit(ProcessPoolExample.cpu_intensive_task, num): num for num in numbers}
            results = []
            
            for future in as_completed(futures):
                num = futures[future]
                try:
                    result_msg, t_start, t_end, (mem, load) = future.result()
                    results.append(result_msg)
                    # print(f"Completed: {result_msg}")
                    
                    if self.logger:
                        self.logger.log_result("cpu_intensive", f"num_{num}", t_start, t_end, mem, load, self.test_run)
                        
                except Exception as e:
                    print(f"Task for {num} failed: {e}")

        end_time = time.time() - start_time
        print(f"All CPU Intentive Tasks Finished - Time elapsed: {end_time:.2f} seconds")
        
        return results

    @staticmethod
    def mixed_task(task_id):
        """Mixed task (CPU + I/O)"""
        start_time = time.time()
        print(f"[PID {os.getpid()}] Mixed task {task_id} started")
        
        cpu_result = sum(i * i for i in range(100000))
        
        time.sleep(random.uniform(0.1, 0.3))
        
        end_time = time.time()
        metrics = get_process_metrics()
        
        msg = f"Task {task_id} completed (CPU: {cpu_result})"
        print(f"[PID {os.getpid()}] {msg}")
        return msg, start_time, end_time, metrics

    def run_mixed_tasks(self, num_tasks=8):
        """Executes mixed tasks"""
        print(f"Running {num_tasks} mixed tasks...")

        start_time = time.time()
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(ProcessPoolExample.mixed_task, i+1) for i in range(num_tasks)]
            
            results = []
            for future in as_completed(futures):
                try:
                    result_msg, t_start, t_end, (mem, load) = future.result()
                    results.append(result_msg)
                    
                    if self.logger:
                        # Extract task_id from msg or pass it back. Let's rely on stored task_id in usage or parse it
                        # easier to just use iteration index or similar. For now just generic name
                        self.logger.log_result("mixed_task", f"task_{results.index(result_msg)+1}", t_start, t_end, mem, load, self.test_run)
                except Exception as e:
                    print(f"Mixed task failed: {e}")

        end_time = time.time() - start_time

        print(f"All Mixed Tasks Finished - Time elapsed: {end_time:.2f} seconds")
        
        return results

    @staticmethod
    def parallel_search(data_chunk, target, chunk_idx):
        """Parallel search in data chunks"""
        start_time = time.time()
        # print(f"[PID {os.getpid()}] Searching in chunk {chunk_idx}")
        time.sleep(0.1)
        found = target in data_chunk
        
        end_time = time.time()
        metrics = get_process_metrics()
        
        msg = f"Found {target} in chunk {chunk_idx}: {data_chunk}" if found else f"Not found in {chunk_idx}"
        return msg, start_time, end_time, metrics

    def run_parallel_search(self, data, target, chunk_size=3):
        """Executes parallel search"""

        start_time = time.time()
        
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(ProcessPoolExample.parallel_search, chunk, target, i) for i, chunk in enumerate(chunks)]
            
            results = []
            for future in as_completed(futures):
                try:
                    result_msg, t_start, t_end, (mem, load) = future.result()
                    results.append(result_msg)
                    if self.logger:
                        self.logger.log_result("parallel_search", f"chunk_{results.index(result_msg)}", t_start, t_end, mem, load, self.test_run)
                except Exception as e:
                    print(f"Search task failed: {e}")

        end_time = time.time() - start_time

        print(f"All Parallel Searches Finished - Time elasped: {end_time:.2f} seconds")
        
        return results

    def shutdown(self):
        """Shuts down the executor"""
        print("ProcessPoolExecutor shutdown")

def cleanup_old_data():
    """Insere um separador nos dados existentes"""
    results_file = "data/processpool_results.csv"
    summary_file = "data/processpool_summary.csv"
    
    for file_path in [results_file, summary_file]:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            try:
                with open(file_path, 'a') as f:
                    f.write('\n')
                print(f"Separator added to {file_path}")
            except Exception as e:
                print(f"Error adding separator: {e}")

def create_processpool_plots():
    """Create a standardized 2x3 visualization grid"""
    results_file = "data/processpool_results.csv"
    
    if not os.path.exists(results_file):
        print(f"Error: {results_file} not found!")
        return
    
    df = read_csv_safe(results_file)
    if df.empty:
        print("No data available for plotting.")
        return

    if 'test_run' not in df.columns:
        df['test_run'] = 1
    df['test_run'] = pd.to_numeric(df['test_run'], errors='coerce')
    
    plt.figure(figsize=(18, 12))
    
    # 1. Average Time per Task ID
    plt.subplot(2, 3, 1)
    if 'task_id' in df.columns and 'elapsed_time' in df.columns:
        try:
            # We might have many task_ids, so filtering top N or just plotting all if few
            avg_times = df.groupby(['test_run', 'task_id'])['elapsed_time'].mean().unstack()
            avg_times.plot(kind='bar', ax=plt.gca(), edgecolor='black', alpha=0.8)
            plt.title('Average Time per Task ID', fontsize=12, fontweight='bold')
            plt.ylabel('Time (seconds)')
            plt.legend(title='Task ID', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')
            plt.grid(True, alpha=0.3, axis='y')
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
            plt.grid(True, alpha=0.3, axis='y')
            for bar in bars.patches:
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), 
                        f'{bar.get_height():.1f}%', ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error Plot 4: {e}")

    # 5. Number of Tasks per Test Run
    plt.subplot(2, 3, 5)
    if 'test_run' in df.columns and 'task_id' in df.columns:
        try:
            task_counts = df.groupby('test_run')['task_id'].count()
            bars = task_counts.plot(kind='bar', color='purple', alpha=0.7, edgecolor='black')
            plt.title('Number of Tasks per Test Run', fontsize=12, fontweight='bold')
            plt.ylabel('Task Count')
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
    os.makedirs('plots', exist_ok=True)
    plt.savefig("plots/processpool_selected_plots.png", dpi=300, bbox_inches='tight')
    plt.close()

def main():
    IsWinOS()
    cleanup_old_data()
    
    NUM_RUNS = 3
    logger = ProcessPoolLogger()

    for i in range(1, NUM_RUNS + 1):
        print(f"\n{'='*40}")
        print(f"STARTING TEST RUN {i} OF {NUM_RUNS}")
        print(f"{'='*40}")

        ppe = ProcessPoolExample(logger=logger, test_run=i)

        print("=== CPU-Intensive Tasks ===")
        numbers = [1000000, 1500000, 2000000, 1200000, 1800000]
        results = ppe.run_cpu_intensive_tasks(numbers)
        
        # print("\n=== Mixed Tasks ===")
        # ppe.run_mixed_tasks(6)
        
        # print("\n=== Parallel Search ===")
        data = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape"]
        search_results = ppe.run_parallel_search(data, "cherry", chunk_size=2)
        # for result in search_results:
        #    print(result)

        ppe.shutdown()
        logger.calculate_summary(test_run=i)

    print("\nGenerating Plots...")
    create_processpool_plots()
    print("All ProcessPool tests completed.")

if __name__ == "__main__":
    main()
