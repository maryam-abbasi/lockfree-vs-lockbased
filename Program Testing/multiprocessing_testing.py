import multiprocessing
from multiprocessing import Process, cpu_count, current_process
import os
import time
import random
import csv  # For writing CSV files
import pandas as pd  # For data analysis and CSV reading
import matplotlib.pyplot as plt  # For creating plots
from datetime import datetime
import psutil  # For memory and system monitoring
import sys
from utils import IsWinOS

def get_memory_usage():
    """Get current process memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

class MultiProcessingLogger:
    """Logger class for storing results in CSV files"""
    def __init__(self, results_file="data/multiprocessing_results.csv", 
                 summary_file="data/multiprocessing_summary.csv"):
        self.results_file = results_file
        self.summary_file = summary_file
        self.results = []
        self.summary_data = []
        
        # Create directories if they don't exist
        os.makedirs("data", exist_ok=True)
        os.makedirs("plots", exist_ok=True)
        
    def log_result(self, test_type, process_id, process_index, start_time, 
                   end_time, memory_usage=None, additional_info=None):
        """Log a single test result and append to CSV file"""
        elapsed_time = end_time - start_time
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Create result dictionary
        result = {
            'timestamp': timestamp,
            'test_type': test_type,
            'process_id': process_id,
            'process_index': process_index,
            'start_time': start_time,
            'end_time': end_time,
            'elapsed_time': elapsed_time,
            'memory_usage_mb': memory_usage,
            'cpu_count': cpu_count(),
            'system_load': psutil.cpu_percent(interval=0.1)
        }
        
        if additional_info:
            result.update(additional_info)
        
        self.results.append(result)
        
        # Check if file exists to write header
        file_exists = os.path.isfile(self.results_file)
        
        # Append result to CSV file
        with open(self.results_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=result.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(result)
        
        return result
    
    def calculate_summary(self):
        """Calculate summary statistics and update summary CSV"""
        if not self.results:
            return
        
        # Convert results to DataFrame for analysis
        df = pd.DataFrame(self.results)
        
        # Calculate summary statistics
        summary = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_tests': len(df),
            'avg_elapsed_time': df['elapsed_time'].mean(),
            'min_elapsed_time': df['elapsed_time'].min(),
            'max_elapsed_time': df['elapsed_time'].max(),
            'std_elapsed_time': df['elapsed_time'].std(),
            'avg_memory_usage': df['memory_usage_mb'].mean() if 'memory_usage_mb' in df.columns else None,
            'avg_system_load': df['system_load'].mean(),
            'cpu_count': cpu_count()
        }
        
        self.summary_data.append(summary)
        
        # Create or update summary CSV file
        df_summary = pd.DataFrame(self.summary_data)
        
        # Load existing data if file exists
        if os.path.isfile(self.summary_file):
            existing_df = pd.read_csv(self.summary_file)
            df_summary = pd.concat([existing_df, df_summary], ignore_index=True)
        
        # Save to CSV file
        df_summary.to_csv(self.summary_file, index=False)
        
        return summary

def worker(index, logger):
    """Worker function for simple tasks"""
    start_time = time.time()
    memory_before = get_memory_usage()
    process_id = os.getpid()
    
    print(f"[PID {os.getpid()}] Process {index} Started.")
    time.sleep(1)
    
    memory_after = get_memory_usage()
    end_time = time.time()
    
    # Log result to CSV
    logger.log_result(
        test_type="simple_worker",
        process_id=process_id,
        process_index=index,
        start_time=start_time,
        end_time=end_time,
        memory_usage=memory_after - memory_before
    )
    
    print(f"[PID {os.getpid()}] Process {index} Finished, Time Elapsed: {end_time-start_time:.2f} seconds")
    return f"Process {index} result"

def cpu_intensive_worker(number, logger, worker_index=None):
    """Worker for CPU-intensive tasks"""
    start_time = time.time()
    memory_before = get_memory_usage()
    process_id = os.getpid()
    
    if worker_index is None:
        worker_index = number
    
    result = sum(i * i for i in range(number))
    
    memory_after = get_memory_usage()
    end_time = time.time()
    
    # Log result to CSV
    logger.log_result(
        test_type="cpu_intensive",
        process_id=process_id,
        process_index=worker_index,
        start_time=start_time,
        end_time=end_time,
        memory_usage=memory_after - memory_before,
        additional_info={'number': number, 'result': result}
    )
    
    print(f"[PID {os.getpid()}] Calculated sum of squares up to {number}: {result} - Time elapsed: {end_time-start_time:.2f} seconds")
    return result

def create_processes(max_processes, logger):
    """Create and execute simple processes"""
    print(f"Creating {max_processes} Processes (1 per CPU).")
    start_time = time.time()
    processes = []

    for i in range(max_processes):
        p = Process(target=worker, args=(i + 1, logger))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    end_time = time.time() - start_time

    print(f"All processes have been finished, time elapsed: {end_time:.2f} seconds")

def create_cpu_intensive_processes(numbers, logger):
    """Create and execute CPU-intensive processes"""
    print("Creating CPU-intensive processes...")
    start_time = time.time()
    processes = []

    for idx, num in enumerate(numbers):
        p = Process(target=cpu_intensive_worker, args=(num, logger, idx+1))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    end_time = time.time() - start_time

    print(f"CPU-intensive processes finished, time elapsed: {end_time:.2f} seconds")

def create_plots():
    """Create visualization plots from CSV data"""
    results_file = "data/multiprocessing_results.csv"
    summary_file = "data/multiprocessing_summary.csv"
    
    if not os.path.exists(results_file):
        print(f"Arquivo {results_file} não encontrado!")
        return
    
    # Load data from CSV file
    df = pd.read_csv(results_file)
    
    # Create figure with 4 subplots
    plt.figure(figsize=(15, 10))
    
    # Plot 1: Time vs Processes (bar chart)
    plt.subplot(2, 2, 1)
    if 'process_index' in df.columns and 'elapsed_time' in df.columns:
        df.groupby('process_index')['elapsed_time'].mean().plot(kind='bar', color='skyblue')
        plt.title('Time vs Processes')
        plt.xlabel('Process Index')
        plt.ylabel('Time (seconds)')
    
    # Plot 2: Memory Usage Distribution (histogram)
    plt.subplot(2, 2, 2)
    if 'memory_usage_mb' in df.columns:
        df['memory_usage_mb'].dropna().plot(kind='hist', color='lightgreen', bins=20)
        plt.title('Memory Usage Distribution')
        plt.xlabel('Memory Usage (MB)')
        plt.ylabel('Frequency')
    
    # Plot 3: System Load Over Time (line chart)
    plt.subplot(2, 2, 3)
    if 'timestamp' in df.columns and 'system_load' in df.columns:
        df['timestamp_dt'] = pd.to_datetime(df['timestamp'])
        df.sort_values('timestamp_dt', inplace=True)
        plt.plot(df['timestamp_dt'], df['system_load'], marker='o', linestyle='-', color='orange')
        plt.title('System Load Over Time')
        plt.xlabel('Time')
        plt.ylabel('System Load (%)')
        plt.xticks(rotation=45)
    
    # Plot 4: Time by Test Type (box plot)
    plt.subplot(2, 2, 4)
    if 'test_type' in df.columns and 'elapsed_time' in df.columns:
        df.boxplot(column='elapsed_time', by='test_type', grid=False)
        plt.title('Time by Test Type')
        plt.suptitle('')
        plt.xlabel('Test Type')
        plt.ylabel('Time (seconds)')
    
    plt.tight_layout()
    
    # Save plot to file with timestamp
    plot_filename = f"plots/multiprocessing_plots_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    plt.show()
    
    # Display summary statistics from CSV
    if os.path.exists(summary_file):
        df_summary = pd.read_csv(summary_file)
        print(f"\nSummary Statistics:")
        print(df_summary.tail())

if __name__ == "__main__":
    IsWinOS()
        
    # Initialize CSV logger
    logger = MultiProcessingLogger()
    
    max_processes = cpu_count()
    print(f"Number of CPUs Available: {max_processes}")

    print("\n=== Basic Multiprocessing Example ===")
    create_processes(max_processes, logger)

    print("\n=== CPU-Bound Processes Example ===")
    create_cpu_intensive_processes([1000000, 2000000, 3000000], logger)
    
    # Calculate and save summary to CSV
    logger.calculate_summary()
    
    print("\n=== Generating Plots ===")
    create_plots()