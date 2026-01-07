import multiprocessing
from multiprocessing import Process, cpu_count, current_process
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
        
        os.makedirs("data", exist_ok=True)
        os.makedirs("plots", exist_ok=True)
        
        self.fieldnames = [
            'timestamp', 'thread_name', 'thread_index', 'start_time', 
            'end_time', 'elapsed_time', 'memory_usage_mb', 'cpu_count',
            'system_load', 'test_run', 'rand_max', 'result', 'calculation_size'
        ]
    
    def write_result_to_csv(self, result_dict):
        """Write a single result to CSV file (safe for multiprocessing)"""
        file_exists = os.path.isfile(self.results_file)
        
        with open(self.results_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            
            if not file_exists or os.path.getsize(self.results_file) == 0:
                writer.writeheader()
            
            # Garantir que todas as colunas estejam presentes
            row_to_write = {field: result_dict.get(field, '') for field in self.fieldnames}
            writer.writerow(row_to_write)
    
    def log_result(self, thread_name, thread_index, start_time, end_time, 
                   memory_usage=None, additional_info=None, test_run=1):
        """Log a single test result and append to CSV file"""
        elapsed_time = end_time - start_time
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Create result dictionary - MESMA estrutura do threading
        result = {
            'timestamp': timestamp,
            'thread_name': thread_name,
            'thread_index': thread_index,
            'start_time': start_time,
            'end_time': end_time,
            'elapsed_time': elapsed_time,
            'memory_usage_mb': memory_usage,
            'cpu_count': cpu_count(),
            'system_load': psutil.cpu_percent(interval=0.1),
            'test_run': test_run
        }
        
        if additional_info:
            result.update(additional_info)
        
        self.results.append(result)
        
        # Write to CSV (safe for multiprocessing)
        self.write_result_to_csv(result)
        
        return result
    
    def calculate_summary(self, test_run=None):
        """Calculate summary statistics and update summary CSV"""
        if not self.results:
            return
        
        # Process performance data using Pandas for statistical aggregation
        df = pd.DataFrame(self.results)
        
        if test_run is not None:
            df = df[df['test_run'] == test_run]
        
        if len(df) == 0:
            return
        
        summary = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_tests': len(df),
            'avg_elapsed_time': round(df['elapsed_time'].mean(), 4),
            'min_elapsed_time': round(df['elapsed_time'].min(), 4),
            'max_elapsed_time': round(df['elapsed_time'].max(), 4),
            'std_elapsed_time': round(df['elapsed_time'].std(), 4),
            'avg_memory_usage': round(df['memory_usage_mb'].mean(), 4) if 'memory_usage_mb' in df.columns else None,
            'avg_system_load': round(df['system_load'].mean(), 4),
            'cpu_count': cpu_count(),
            'total_threads': df['thread_index'].nunique(),
            'test_run': test_run if test_run else 'all'
        }
        
        self.summary_data.append(summary)
        
        # Append current summary snippet to CSV, maintaining potential visual separators
        df_single = pd.DataFrame([summary])
        file_exists = os.path.isfile(self.summary_file) and os.path.getsize(self.summary_file) > 0
        df_single.to_csv(self.summary_file, mode='a', index=False, header=not file_exists)
        
        return summary
    
    def calculate_final_summary(self):
        """Calcula a média das 5 execuções"""
        if not self.results:
            return
        
        # Aggregate across multiple independent test runs using Pandas
        df = pd.DataFrame(self.results)
        
        test_runs = df['test_run'].unique()
        
        if len(test_runs) == 0:
            return
        
        run_summaries = []
        for run in test_runs:
            run_df = df[df['test_run'] == run]
            if len(run_df) > 0:
                summary = {
                    'test_run': run,
                    'avg_elapsed_time': round(run_df['elapsed_time'].mean(), 4),
                    'avg_memory_usage': round(run_df['memory_usage_mb'].mean(), 4) if 'memory_usage_mb' in run_df.columns else None,
                    'avg_system_load': round(run_df['system_load'].mean(), 4),
                    'total_threads': run_df['thread_index'].nunique()
                }
                run_summaries.append(summary)
        
        if run_summaries:
            df_runs = pd.DataFrame(run_summaries)
            final_summary = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'total_test_runs': len(test_runs),
                'final_avg_elapsed_time': round(df_runs['avg_elapsed_time'].mean(), 4),
                'final_min_elapsed_time': round(df_runs['avg_elapsed_time'].min(), 4),
                'final_max_elapsed_time': round(df_runs['avg_elapsed_time'].max(), 4),
                'final_std_elapsed_time': round(df_runs['avg_elapsed_time'].std(), 4),
                'final_avg_memory_usage': round(df_runs['avg_memory_usage'].mean(), 4) if 'avg_memory_usage' in df_runs.columns else None,
                'final_avg_system_load': round(df_runs['avg_system_load'].mean(), 4),
                'avg_threads_per_run': round(df_runs['total_threads'].mean(), 4)
            }
            
            final_file = "data/multiprocessing_final_summary.csv"
            
            # Append global experiment statistics snippet to the final summary report
            final_df = pd.DataFrame([final_summary])
            file_exists = os.path.isfile(final_file) and os.path.getsize(final_file) > 0
            final_df.to_csv(final_file, mode='a', index=False, header=not file_exists)
            
            return final_summary

def worker_function(index, test_run, results_file, fieldnames):
    """Worker function - standalone function for multiprocessing"""
    start_time = time.time()
    memory_before = get_memory_usage()
    
    # Usar nome do processo atual
    process_name = f"Process-{index}-Run{test_run}"
    process_id = os.getpid()
    
    print(f"[PID {process_id}] Worker {index} started (Test Run {test_run})")
    
    # MESMO cálculo que threading: soma de 1 até rand_max
    rand_max = random.randint(1000000, 4000000)
    result = sum(range(1, rand_max))
    
    memory_after = get_memory_usage()
    end_time = time.time()
    
    # Calcular métricas
    elapsed_time = end_time - start_time
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Criar resultado
    result_dict = {
        'timestamp': timestamp,
        'thread_name': process_name,
        'thread_index': index,
        'start_time': start_time,
        'end_time': end_time,
        'elapsed_time': elapsed_time,
        'memory_usage_mb': memory_after - memory_before,
        'cpu_count': cpu_count(),
        'system_load': psutil.cpu_percent(interval=0.1),
        'test_run': test_run,
        'rand_max': rand_max,
        'result': result,
        'calculation_size': rand_max
    }
    
    # Escrever diretamente no CSV
    file_exists = os.path.isfile(results_file)
    
    with open(results_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not file_exists or os.path.getsize(results_file) == 0:
            writer.writeheader()
        
        # Garantir que todas as colunas estejam presentes
        row_to_write = {field: result_dict.get(field, '') for field in fieldnames}
        writer.writerow(row_to_write)
    
    print(f"Task {index} (Run {test_run}): Sum is {result}")
    print(f"[PID {process_id}] Worker {index} finished - Time elapsed: {elapsed_time:.2f} seconds")
    
    return f"Result from worker {index}: {result}"

class MultiProcess:
    def __init__(self, test_run=1):
        self.processes = []
        self.test_run = test_run
        
        # Fieldnames para passar para os workers
        self.fieldnames = [
            'timestamp', 'thread_name', 'thread_index', 'start_time', 
            'end_time', 'elapsed_time', 'memory_usage_mb', 'cpu_count',
            'system_load', 'test_run', 'rand_max', 'result', 'calculation_size'
        ]
        
        # Criar diretórios se não existirem
        os.makedirs("data", exist_ok=True)
        os.makedirs("plots", exist_ok=True)

    def createMultipleProcesses(self, num_processes=5):
        """Create and execute processes - MESMA estrutura do threading"""
        start_all_time = time.time()
        print()
        print(50*"=")
        print(f"Test Run {self.test_run}: Creating {num_processes} processes...")
        print(50*"=")
        print()
        
        self.processes = []
        
        # Arquivo de resultados
        results_file = "data/multiprocessing_results.csv"
        
        for i in range(num_processes):
            # Passar apenas argumentos simples (pickle-safe)
            process = Process(
                target=worker_function,
                args=(i+1, self.test_run, results_file, self.fieldnames)
            )
            self.processes.append(process)
            process.start()

        for process in self.processes:
            process.join()

        end_all_time = time.time() - start_all_time
        print()
        print(f"Test Run {self.test_run} Finished - Total Time: {end_all_time:.2f} seconds")
        print(50*"=")
        print()
        
        return end_all_time

def read_csv_safe(file_path):
    """Lê um arquivo CSV de forma segura, lidando com possíveis inconsistências"""
    try:
        # Load raw CSV data using Pandas and apply precision rounding to specific metrics
        df = pd.read_csv(file_path)
        
        for col in df.select_dtypes(include=[np.number]).columns:
            if col not in ['test_run', 'thread_index', 'cpu_count', 'rand_max', 'result', 'calculation_size']:
                df[col] = df[col].round(4)
        
        return df
    except pd.errors.ParserError as e:
        print(f"Warning: Parser error reading {file_path}: {e}")
        
        try:
            # Salvage data corruption by skipping malformed lines during parsing
            df = pd.read_csv(file_path, on_bad_lines='skip')
            print(f"Successfully read {len(df)} rows (some rows may have been skipped)")
            
            for col in df.select_dtypes(include=[np.number]).columns:
                if col not in ['test_run', 'thread_index', 'cpu_count', 'rand_max', 'result', 'calculation_size']:
                    df[col] = df[col].round(4)
            
            return df
        except Exception as e2:
            print(f"Error reading with skip: {e2}")
            
            try:
                # Manual fallback filtering when automated skipped parsing fails
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
                    
                    for col in df.select_dtypes(include=[np.number]).columns:
                        if col not in ['test_run', 'thread_index', 'cpu_count', 'rand_max', 'result', 'calculation_size']:
                            df[col] = df[col].round(4)
                    
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

def create_processing_plots_multiple_runs():
    """Create a standardized 2x3 visualization grid for process performance analysis"""
    results_file = "data/multiprocessing_results.csv"
    summary_file = "data/multiprocessing_summary.csv"
    
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
    
    # 1. Average Time per Thread Index (Using process index as proxy)
    plt.subplot(2, 3, 1)
    if 'thread_index' in df.columns and 'elapsed_time' in df.columns:
        try:
            avg_times = df.groupby(['test_run', 'thread_index'])['elapsed_time'].mean().unstack()
            bars = avg_times.plot(kind='bar', ax=plt.gca(), edgecolor='black', alpha=0.8)
            plt.title('Average Time per Thread Index', fontsize=12, fontweight='bold')
            plt.ylabel('Time (seconds)')
            plt.legend(title='Process Index', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')
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

    # 5. Number of Threads per Test Run (Process count as proxy)
    plt.subplot(2, 3, 5)
    if 'test_run' in df.columns and 'thread_index' in df.columns:
        try:
            process_counts = df.groupby('test_run')['thread_index'].nunique()
            bars = process_counts.plot(kind='bar', color='purple', alpha=0.7, edgecolor='black')
            plt.title('Number of Threads per Test Run', fontsize=12, fontweight='bold')
            plt.ylabel('Process Count')
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
    plt.savefig("plots/multiprocessing_selected_plots.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    if os.path.exists(summary_file):
        try:
            df_summary = read_csv_safe(summary_file)
            if not df_summary.empty:
                print(f"\nMultiprocessing Summary Statistics:")
                print(df_summary.tail())
        except Exception as e:
            print(f"Error reading summary file: {e}")

def cleanup_old_data():
    """Insere um separador nos dados existentes em vez de limpar"""
    results_file = "data/multiprocessing_results.csv"
    summary_file = "data/multiprocessing_summary.csv"
    
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
    
    logger = MultiProcessingLogger()
    
    print("\n" + "="*60)
    print("MULTIPLE TEST RUNS EXPERIMENT - MULTIPROCESSING")
    print("="*60)
    
    process_counts = [4, 8, 16]
    all_total_times = []
    
    for test_run in range(1, 6):
        print(f"\n{'#'*60}")
        print(f"STARTING TEST RUN {test_run} OF 5")
        print(f"{'#'*60}")
        
        run_times = []
        
        for count in process_counts:
            print(f"\n{'='*40}")
            print(f"Test Run {test_run}: Testing with {count} processes")
            print(f"{'='*40}")
            
            # Criar MultiProcess sem logger (evita problemas de serialização)
            mp = MultiProcess(test_run=test_run)
            total_time = mp.createMultipleProcesses(count)
            run_times.append(round(total_time, 4))  # ARREDONDADO 4 casas
            
            try:
                df = pd.read_csv("data/multiprocessing_results.csv")
                if not df.empty:
                    run_df = df[df['test_run'] == test_run]
                    if len(run_df) > 0:
                        # Manual calculation of test run statistics using Pandas
                        summary = {
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'total_tests': len(run_df),
                            'avg_elapsed_time': round(run_df['elapsed_time'].mean(), 4),
                            'min_elapsed_time': round(run_df['elapsed_time'].min(), 4),
                            'max_elapsed_time': round(run_df['elapsed_time'].max(), 4),
                            'std_elapsed_time': round(run_df['elapsed_time'].std(), 4),
                            'avg_memory_usage': round(run_df['memory_usage_mb'].mean(), 4) if 'memory_usage_mb' in run_df.columns else None,
                            'avg_system_load': round(run_df['system_load'].mean(), 4),
                            'cpu_count': cpu_count(),
                            'total_threads': run_df['thread_index'].nunique(),
                            'test_run': test_run
                        }
                        
                        logger.summary_data.append(summary)
                        
                        df_summary = pd.DataFrame(logger.summary_data)
                        df_summary.to_csv(logger.summary_file, index=False)
            except Exception as e:
                print(f"Warning: Could not calculate summary for run {test_run}: {e}")
            
            time.sleep(1)
        
        all_total_times.append(run_times)
        
        print(f"\nTest Run {test_run} Results:")
        for i, count in enumerate(process_counts):
            print(f"  {count} processes: {run_times[i]:.4f} seconds")  # 4 casas decimais
    
    # Calcular summary final com médias das 5 execuções - MESMO
    print(f"\n{'#'*60}")
    print("CALCULATING FINAL SUMMARY (AVERAGE OF 5 RUNS)")
    print(f"{'#'*60}")
    
    final_summary = logger.calculate_final_summary()
    
    if final_summary:
        print(f"\nFinal Summary Statistics (Average of 5 test runs):")
        for key, value in final_summary.items():
            if key != 'timestamp':
                print(f"  {key}: {value}")
    
    # Gerar gráficos consolidados das 5 execuções - APENAS OS SOLICITADOS
    print(f"\n{'#'*60}")
    print("GENERATING SELECTED PLOTS FOR ALL 5 TEST RUNS")
    print(f"{'#'*60}")
    
    create_processing_plots_multiple_runs()
    
    # Mostrar tempos médios por configuração - MESMO
    print(f"\n{'#'*60}")
    print("AVERAGE EXECUTION TIMES (5 RUNS)")
    print(f"{'#'*60}")
    
    if all_total_times:
        # Converter para cálculos
        times_array = np.array(all_total_times)
        
        for i, count in enumerate(process_counts):
            avg_time = np.mean(times_array[:, i])
            std_time = np.std(times_array[:, i])
            min_time = np.min(times_array[:, i])
            max_time = np.max(times_array[:, i])
            
            print(f"\n{count} processes:")
            print(f"  Average: {avg_time:.4f} ± {std_time:.4f} seconds")  # 4 casas
            print(f"  Range: {min_time:.4f} - {max_time:.4f} seconds")    # 4 casas
            print(f"  Variation: {((max_time-min_time)/avg_time*100):.2f}%")  # 2 casas para porcentagem
    
    print(f"\n{'='*60}")
    print("EXPERIMENT COMPLETED SUCCESSFULLY")
    print(f"Data saved to: /data/multiprocessing_results.csv")
    print(f"Summary saved to: /data/multiprocessing_summary.csv")
    print(f"Plots saved to: /plots/")
    print(f"{'='*60}")

if __name__ == "__main__":
    """Check if the Operating System is Windows"""
    IsWinOS()

    """Configure method of inicialization to multiprocessing"""
    multiprocessing.set_start_method('spawn', force=True)

    """ Main Function for MultiProcessing Testing """
    main()