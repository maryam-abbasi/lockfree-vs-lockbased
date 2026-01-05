import threading
from threading import Thread, current_thread
import os
import time
import random
import csv
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import psutil
from utils import IsWinOS

def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

class MultiThreadLogger:
    def __init__(self, results_file="data/multithreading_results.csv", 
                 summary_file="data/multithreading_summary.csv"):
        self.results_file = results_file
        self.summary_file = summary_file
        self.results = []
        self.summary_data = []
        
        os.makedirs("data", exist_ok=True)
        os.makedirs("plots", exist_ok=True)
        
    def log_result(self, thread_name, thread_index, start_time, end_time, 
                   memory_usage=None, additional_info=None):
        elapsed_time = end_time - start_time
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        result = {
            'timestamp': timestamp,
            'thread_name': thread_name,
            'thread_index': thread_index,
            'start_time': start_time,
            'end_time': end_time,
            'elapsed_time': elapsed_time,
            'memory_usage_mb': memory_usage,
            'cpu_count': os.cpu_count(),
            'system_load': psutil.cpu_percent(interval=0.1)
        }
        
        if additional_info:
            result.update(additional_info)
        
        self.results.append(result)
        
        file_exists = os.path.isfile(self.results_file)
        
        with open(self.results_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=result.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(result)
        
        return result
    
    def calculate_summary(self):
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        summary = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_tests': len(df),
            'avg_elapsed_time': df['elapsed_time'].mean(),
            'min_elapsed_time': df['elapsed_time'].min(),
            'max_elapsed_time': df['elapsed_time'].max(),
            'std_elapsed_time': df['elapsed_time'].std(),
            'avg_memory_usage': df['memory_usage_mb'].mean() if 'memory_usage_mb' in df.columns else None,
            'avg_system_load': df['system_load'].mean(),
            'cpu_count': os.cpu_count(),
            'total_threads': df['thread_index'].nunique()
        }
        
        self.summary_data.append(summary)
        
        df_summary = pd.DataFrame(self.summary_data)
        
        if os.path.isfile(self.summary_file):
            existing_df = pd.read_csv(self.summary_file)
            df_summary = pd.concat([existing_df, df_summary], ignore_index=True)
        
        df_summary.to_csv(self.summary_file, index=False)
        
        return summary

class MultiThread:
    def __init__(self, logger):
        self.threads = []
        self.logger = logger

    def worker(self, index):
        start_time = time.time()
        memory_before = get_memory_usage()
        thread_name = current_thread().name
        
        print(f"[{thread_name}] Worker {index} started")
        
        rand_max = random.randint(1000000, 4000000)
        result = sum(range(1, rand_max))
        
        memory_after = get_memory_usage()
        end_time = time.time()
        
        self.logger.log_result(
            thread_name=thread_name,
            thread_index=index,
            start_time=start_time,
            end_time=end_time,
            memory_usage=memory_after - memory_before,
            additional_info={
                'rand_max': rand_max, 
                'result': result,
                'calculation_size': rand_max
            }
        )
        
        print(f"Task {index}: Sum is {result}")
        print(f"[{thread_name}] Worker {index} finished - Time elapsed for Thread {index}: {end_time-start_time:.2f} seconds")
        return f"Result from worker {index}: {result}"

    def createMultipleThreads(self, num_threads=5):
        start_all_time = time.time()
        print()
        print(50*"=")
        print(f"Creating {num_threads} threads...")
        print()
        
        self.threads = []
        
        for i in range(num_threads):
            thread = Thread(target=self.worker, args=(i+1,), name=f"Worker-{i+1}")
            self.threads.append(thread)
            thread.start()

        for thread in self.threads:
            thread.join()

        end_all_time = time.time() - start_all_time
        print()
        print(f"All Multiple Threads Finished Successfully - Time elapsed: {end_all_time:.2f} seconds")
        print(50*"=")
        print()
        
        return end_all_time

def create_threading_plots():
    results_file = "data/multithreading_results.csv"
    summary_file = "data/multithreading_summary.csv"
    
    if not os.path.exists(results_file):
        print(f"Arquivo {results_file} não encontrado!")
        return
    
    df = pd.read_csv(results_file)
    
    plt.figure(figsize=(15, 10))
    
    plt.subplot(2, 2, 1)
    if 'thread_index' in df.columns and 'elapsed_time' in df.columns:
        df.groupby('thread_index')['elapsed_time'].mean().plot(kind='bar', color='skyblue')
        plt.title('Time vs Thread Index')
        plt.xlabel('Thread Index')
        plt.ylabel('Time (seconds)')
    
    plt.subplot(2, 2, 2)
    if 'memory_usage_mb' in df.columns:
        df['memory_usage_mb'].dropna().plot(kind='hist', color='lightgreen', bins=20)
        plt.title('Memory Usage Distribution (Threading)')
        plt.xlabel('Memory Usage (MB)')
        plt.ylabel('Frequency')
    
    plt.subplot(2, 2, 3)
    if 'timestamp' in df.columns and 'system_load' in df.columns:
        df['timestamp_dt'] = pd.to_datetime(df['timestamp'])
        df.sort_values('timestamp_dt', inplace=True)
        plt.plot(df['timestamp_dt'], df['system_load'], marker='o', linestyle='-', color='orange')
        plt.title('System Load Over Time (Threading)')
        plt.xlabel('Time')
        plt.ylabel('System Load (%)')
        plt.xticks(rotation=45)
    
    plt.subplot(2, 2, 4)
    if 'calculation_size' in df.columns and 'elapsed_time' in df.columns:
        plt.scatter(df['calculation_size'], df['elapsed_time'], alpha=0.6, color='red')
        plt.title('Calculation Size vs Execution Time')
        plt.xlabel('Calculation Size')
        plt.ylabel('Time (seconds)')
    
    plt.tight_layout()
    
    plot_filename = f"plots/multithreading_plots_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    plt.show()
    
    if os.path.exists(summary_file):
        df_summary = pd.read_csv(summary_file)
        print(f"\nThreading Summary Statistics:")
        print(df_summary.tail())

if __name__ == "__main__":
    IsWinOS()

    logger = MultiThreadLogger()
    
    print("\n=== Multiple Threads Example ===")
    mt = MultiThread(logger)
    
    thread_counts = [4, 8, 16]
    total_times = []
    
    for count in thread_counts:
        print(f"\n{'#'*60}")
        print(f"Testing with {count} threads")
        print(f"{'#'*60}")
        
        total_time = mt.createMultipleThreads(count)
        total_times.append(total_time)
        time.sleep(1)
    
    logger.calculate_summary()
    
    print("\n=== Generating Threading Plots ===")
    create_threading_plots()
    
    print(f"\nTotal execution times:")
    for i, count in enumerate(thread_counts):
        print(f"{count} threads: {total_times[i]:.2f} seconds")