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
import numpy as np

def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

class MultiThreadLogger:
    def __init__(self, results_file="data/threading_results.csv", 
                 summary_file="data/threading_summary.csv"):
        self.results_file = results_file
        self.summary_file = summary_file
        self.results = []
        self.summary_data = []
        
        os.makedirs("data", exist_ok=True)
        os.makedirs("plots", exist_ok=True)
        
        self.fieldnames = [
            'timestamp', 'thread_name', 'thread_index', 'start_time', 
            'end_time', 'elapsed_time', 'memory_usage_mb', 'cpu_count',
            'system_load', 'test_run', 'rand_max', 'result', 'calculation_size'
        ]
    
    def write_result_to_csv(self, result_dict):
        """Write a single result to CSV file (safe for threading)"""
        file_exists = os.path.isfile(self.results_file)
        
        # Usar lock para escrita thread-safe
        with threading.Lock():
            with open(self.results_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                
                if not file_exists or os.path.getsize(self.results_file) == 0:
                    writer.writeheader()
                
                # Garantir que todas as colunas estejam presentes
                row_to_write = {field: result_dict.get(field, '') for field in self.fieldnames}
                writer.writerow(row_to_write)
        
    def log_result(self, thread_name, thread_index, start_time, end_time, 
                   memory_usage=None, additional_info=None, test_run=1):
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
            'system_load': psutil.cpu_percent(interval=0.1),
            'test_run': test_run
        }
        
        if additional_info:
            result.update(additional_info)
        
        self.results.append(result)
        
        self.write_result_to_csv(result)
        
        return result
    
    def calculate_summary(self, test_run=None):
        """Calcula estatísticas sumárias para um test_run específico"""
        if not self.results:
            return
        
        try:
            # Load threading results into a Pandas DataFrame for statistical aggregation
            df = pd.read_csv(self.results_file)
        except:
            df = pd.DataFrame(self.results)
        
        if df.empty:
            return
        
        if test_run is not None:
            df = df[df['test_run'] == test_run]
        
        if len(df) == 0:
            return
        
        # Compute performance metrics using vectorized operations in Pandas
        summary = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'test_run': test_run,
            'total_tests': len(df),
            'total_threads_used': df['thread_index'].nunique(),
            'avg_elapsed_time': df['elapsed_time'].mean(),
            'min_elapsed_time': df['elapsed_time'].min(),
            'max_elapsed_time': df['elapsed_time'].max(),
            'std_elapsed_time': df['elapsed_time'].std(),
            'avg_memory_usage': df['memory_usage_mb'].mean() if 'memory_usage_mb' in df.columns else None,
            'avg_system_load': df['system_load'].mean(),
            'avg_calculation_size': df['calculation_size'].mean() if 'calculation_size' in df.columns else None,
            'cpu_count': os.cpu_count()
        }
        
        self.summary_data.append(summary)
        
        # Append current summary snippet to CSV, maintaining potential visual separators
        df_single = pd.DataFrame([summary])
        file_exists = os.path.isfile(self.summary_file) and os.path.getsize(self.summary_file) > 0
        df_single.to_csv(self.summary_file, mode='a', index=False, header=not file_exists)
        
        return summary
    
    # Method deprecated - logic moved to calculate_summary for safer appending
    def save_summary_to_csv(self):
        """Salva todos os summaries no arquivo CSV"""
        pass

class MultiThread:
    def __init__(self, logger, test_run=1):
        self.threads = []
        self.logger = logger
        self.test_run = test_run

    def worker(self, index):
        start_time = time.time()
        memory_before = get_memory_usage()
        thread_name = current_thread().name
        
        print(f"[{thread_name}] Worker {index} started (Test Run {self.test_run})")
        
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
            },
            test_run=self.test_run
        )
        
        print(f"Task {index} (Run {self.test_run}): Sum is {result}")
        print(f"[{thread_name}] Worker {index} finished - Time elapsed: {end_time-start_time:.2f} seconds")
        return f"Result from worker {index}: {result}"

    def createMultipleThreads(self, num_threads=5):
        start_all_time = time.time()
        print()
        print(50*"=")
        print(f"Test Run {self.test_run}: Creating {num_threads} threads...")
        print(50*"=")
        print()
        
        self.threads = []
        
        for i in range(num_threads):
            thread = Thread(target=self.worker, args=(i+1,), name=f"Worker-{i+1}-Run{self.test_run}")
            self.threads.append(thread)
            thread.start()

        for thread in self.threads:
            thread.join()

        end_all_time = time.time() - start_all_time
        print()
        print(f"Test Run {self.test_run} Finished - Total Time: {end_all_time:.2f} seconds")
        print(50*"=")
        print()
        
        return end_all_time

def read_csv_safe(file_path):
    """Lê um arquivo CSV de forma segura, lidando com possíveis inconsistências"""
    try:
        # Load measurement data with Pandas, ensuring robust handling of potential CSV corruptions
        return pd.read_csv(file_path)
    except pd.errors.ParserError as e:
        print(f"Warning: Parser error reading {file_path}: {e}")
        
        try:
            # Attempt to salvage data by skipping malformed lines during parsing
            df = pd.read_csv(file_path, on_bad_lines='skip')
            print(f"Successfully read {len(df)} rows (some rows may have been skipped)")
            return df
        except Exception as e2:
            print(f"Error reading with skip: {e2}")
            
            try:
                # Fallback to manual line filtering if automated skipping fails
                with open(file_path, 'r') as f:
                    lines = f.readlines()
                
                if len(lines) > 0:
                    header = lines[0].strip().split(',')
                    num_columns = len(header)
                    
                    valid_lines = [line for line in lines if len(line.strip().split(',')) == num_columns]
                    
                    temp_file = "temp_fixed.csv"
                    with open(temp_file, 'w') as f:
                        f.writelines(valid_lines)
                    
                    df = pd.read_csv(temp_file)
                    os.remove(temp_file)
                    return df
            except Exception as e3:
                print(f"Failed to read CSV file: {e3}")
                return pd.DataFrame()
    except FileNotFoundError:
        print(f"File {file_path} not found!")
        return pd.DataFrame()
    except Exception as e:
        print(f"Unexpected error reading {file_path}: {e}")
        return pd.DataFrame()

def create_threading_plots_multiple_runs():
    """Create a standardized 2x3 visualization grid for thread performance analysis"""
    results_file = "data/threading_results.csv"
    summary_file = "data/threading_summary.csv"
    
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
    
    # 1. Average Time per Thread Index
    plt.subplot(2, 3, 1)
    if 'thread_index' in df.columns and 'elapsed_time' in df.columns:
        try:
            avg_times = df.groupby(['test_run', 'thread_index'])['elapsed_time'].mean().unstack()
            bars = avg_times.plot(kind='bar', ax=plt.gca(), edgecolor='black', alpha=0.8)
            plt.title('Average Time per Thread Index', fontsize=12, fontweight='bold')
            plt.ylabel('Time (seconds)')
            plt.legend(title='Thread Index', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')
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

    # 5. Number of Threads per Test Run
    plt.subplot(2, 3, 5)
    if 'test_run' in df.columns and 'thread_index' in df.columns:
        try:
            thread_counts = df.groupby('test_run')['thread_index'].nunique()
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
    plt.savefig("plots/threading_selected_plots.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    if os.path.exists(summary_file):
        try:
            df_summary = read_csv_safe(summary_file)
            if not df_summary.empty:
                print(f"\nThreading Summary Statistics:")
                print(df_summary.tail())
        except Exception as e:
            print(f"Error reading summary file: {e}")
    
    if os.path.exists(summary_file):
        try:
            df_summary = read_csv_safe(summary_file)
            if not df_summary.empty:
                print(f"\nThreading Summary Statistics:")
                print(df_summary.to_string(index=False))
        except Exception as e:
            print(f"Error reading summary file: {e}")

def cleanup_old_data():
    """Insere um separador nos dados existentes em vez de limpar"""
    results_file = "data/threading_results.csv"
    summary_file = "data/threading_summary.csv"
    
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
    
    # Limpar dados antigos se necessário
    cleanup_old_data()
    
    logger = MultiThreadLogger()
    
    print("\n" + "="*60)
    print("MULTIPLE TEST RUNS EXPERIMENT - THREADING")
    print("="*60)
    
    thread_counts = [4, 8, 16]
    all_total_times = []
    
    for test_run in range(1, 6):
        print(f"\n{'#'*60}")
        print(f"STARTING TEST RUN {test_run} OF 5")
        print(f"{'#'*60}")
        
        run_times = []
        
        for count in thread_counts:
            print(f"\n{'='*40}")
            print(f"Test Run {test_run}: Testing with {count} threads")
            print(f"{'='*40}")
            
            mt = MultiThread(logger, test_run=test_run)
            total_time = mt.createMultipleThreads(count)
            run_times.append(total_time)
            
            logger.calculate_summary(test_run=test_run)
            
            time.sleep(1)
        
        all_total_times.append(run_times)
        
        print(f"\nTest Run {test_run} Results:")
        for i, count in enumerate(thread_counts):
            print(f"  {count} threads: {run_times[i]:.2f} seconds")
    
    print(f"\n{'#'*60}")
    print("FINALIZING SUMMARY FILE")
    print(f"{'#'*60}")
    
    try:
        # Load accumulated summary data to compute longitudinal averages
        df_summary = pd.read_csv(logger.summary_file)
        
        if not df_summary.empty:
            final_summary = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'test_run': 'OVERALL_AVERAGE',
                'total_tests': df_summary['total_tests'].sum(),
                'total_threads_used': df_summary['total_threads_used'].mean(),
                'avg_elapsed_time': df_summary['avg_elapsed_time'].mean(),
                'min_elapsed_time': df_summary['min_elapsed_time'].min(),
                'max_elapsed_time': df_summary['max_elapsed_time'].max(),
                'std_elapsed_time': df_summary['std_elapsed_time'].mean(),
                'avg_memory_usage': df_summary['avg_memory_usage'].mean() if 'avg_memory_usage' in df_summary.columns else None,
                'avg_system_load': df_summary['avg_system_load'].mean(),
                'avg_calculation_size': df_summary['avg_calculation_size'].mean() if 'avg_calculation_size' in df_summary.columns else None,
                'cpu_count': os.cpu_count()
            }
            
            # Append global experiment statistics to the final summary report
            df_final = pd.DataFrame([final_summary])
            df_summary = pd.concat([df_summary, df_final], ignore_index=True)
            
            df_summary.to_csv(logger.summary_file, index=False)
            
            print(f"\nOverall Average Statistics:")
            for key, value in final_summary.items():
                if key not in ['timestamp', 'test_run']:
                    print(f"  {key}: {value}")
    except Exception as e:
        print(f"Error calculating overall averages: {e}")
    
    # Gerar gráficos consolidados das 5 execuções
    print(f"\n{'#'*60}")
    print("GENERATING SELECTED PLOTS FOR ALL 5 TEST RUNS")
    print(f"{'#'*60}")
    
    create_threading_plots_multiple_runs()
    
    # Mostrar tempos médios por configuração
    print(f"\n{'#'*60}")
    print("AVERAGE EXECUTION TIMES (5 RUNS)")
    print(f"{'#'*60}")
    
    if all_total_times:
        # Converter para cálculos
        times_array = np.array(all_total_times)
        
        for i, count in enumerate(thread_counts):
            avg_time = np.mean(times_array[:, i])
            std_time = np.std(times_array[:, i])
            min_time = np.min(times_array[:, i])
            max_time = np.max(times_array[:, i])
            
            print(f"\n{count} threads:")
            print(f"  Average: {avg_time:.2f} ± {std_time:.2f} seconds")
            print(f"  Range: {min_time:.2f} - {max_time:.2f} seconds")
            print(f"  Variation: {((max_time-min_time)/avg_time*100):.1f}%")
    
    print(f"\n{'='*60}")
    print("EXPERIMENT COMPLETED SUCCESSFULLY")
    print(f"Data saved to: /data/threading_results.csv")
    print(f"Summary saved to: /data/threading_summary.csv")
    print(f"Plots saved to: /plots/")
    print(f"{'='*60}")

if __name__ == "__main__":
    """Check if the Operating System is Windows"""
    IsWinOS()

    """ Main Function fo Threading Testing """
    main()