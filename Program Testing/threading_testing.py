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
        
        # Definir os fieldnames padrão incluindo test_run
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
        
        # Escrever no CSV (thread-safe)
        self.write_result_to_csv(result)
        
        return result
    
    def calculate_summary(self, test_run=None):
        """Calcula estatísticas sumárias para um test_run específico"""
        if not self.results:
            return
        
        # Ler dados do arquivo CSV para garantir dados completos
        try:
            df = pd.read_csv(self.results_file)
        except:
            # Se não conseguir ler o CSV, usar dados em memória
            df = pd.DataFrame(self.results)
        
        if df.empty:
            return
        
        # Se especificar test_run, calcula apenas para essa execução
        if test_run is not None:
            df = df[df['test_run'] == test_run]
        
        if len(df) == 0:
            return
        
        # Calcular estatísticas para este test_run
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
        
        # Adicionar ao summary_data
        self.summary_data.append(summary)
        
        # Salvar/atualizar o arquivo de summary
        self.save_summary_to_csv()
        
        return summary
    
    def save_summary_to_csv(self):
        """Salva todos os summaries no arquivo CSV"""
        if not self.summary_data:
            return
        
        df_summary = pd.DataFrame(self.summary_data)
        
        # Salvar no arquivo CSV (sobrescrever)
        df_summary.to_csv(self.summary_file, index=False)

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
        # Primeiro tentar ler normalmente
        return pd.read_csv(file_path)
    except pd.errors.ParserError as e:
        print(f"Warning: Parser error reading {file_path}: {e}")
        print("Trying to read with error handling...")
        
        # Tentar ler ignorando linhas problemáticas
        try:
            df = pd.read_csv(file_path, on_bad_lines='skip')
            print(f"Successfully read {len(df)} rows (some rows may have been skipped)")
            return df
        except Exception as e2:
            print(f"Error reading with skip: {e2}")
            
            # Última tentativa: ler manualmente
            try:
                with open(file_path, 'r') as f:
                    lines = f.readlines()
                
                if len(lines) > 0:
                    # Tentar determinar o número correto de colunas da primeira linha
                    header = lines[0].strip().split(',')
                    num_columns = len(header)
                    
                    # Filtrar linhas com o número correto de colunas
                    valid_lines = []
                    for i, line in enumerate(lines):
                        if len(line.strip().split(',')) == num_columns:
                            valid_lines.append(line)
                        else:
                            print(f"Skipping line {i+1}: incorrect number of columns")
                    
                    # Salvar temporariamente e ler
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
    """Create visualization plots from CSV data - APENAS OS 6 GRÁFICOS SOLICITADOS"""
    results_file = "data/threading_results.csv"
    summary_file = "data/threading_summary.csv"
    
    if not os.path.exists(results_file):
        print(f"Arquivo {results_file} não encontrado!")
        return
    
    # Usar função segura para ler o CSV
    df = read_csv_safe(results_file)
    
    if df.empty:
        print("No data to plot!")
        return
    
    # Verificar se temos dados de múltiplas execuções
    if 'test_run' not in df.columns:
        print("Warning: 'test_run' column not found in data. Creating default values...")
        # Criar test_run padrão se não existir
        df['test_run'] = 1
    
    # Garantir que test_run seja numérico
    df['test_run'] = pd.to_numeric(df['test_run'], errors='coerce')
    
    # Criar figura com 2x3 grid (6 gráficos)
    plt.figure(figsize=(18, 12))
    
    # 1. Gráfico: Average Time per Thread Index
    plt.subplot(2, 3, 1)
    if 'thread_index' in df.columns and 'elapsed_time' in df.columns and 'test_run' in df.columns:
        try:
            # Calcular médias por thread_index e test_run
            avg_times = df.groupby(['test_run', 'thread_index'])['elapsed_time'].mean().unstack()
            avg_times.plot(kind='bar', ax=plt.gca())
            plt.title('Average Time per Thread Index', fontsize=12, fontweight='bold')
            plt.xlabel('Test Run', fontsize=10)
            plt.ylabel('Average Time (seconds)', fontsize=10)
            plt.legend(title='Thread Index', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3, axis='y')
        except Exception as e:
            print(f"Error creating plot 1: {e}")
            plt.text(0.5, 0.5, 'Error creating plot\nAverage Time per Thread Index', 
                    ha='center', va='center', fontsize=10)
    
    # 2. Gráfico: Average Execution Time per Test Run
    plt.subplot(2, 3, 2)
    if 'test_run' in df.columns and 'elapsed_time' in df.columns:
        try:
            avg_by_run = df.groupby('test_run')['elapsed_time'].mean()
            avg_by_run.plot(kind='line', marker='o', color='blue', linewidth=2, markersize=8)
            plt.title('Average Execution Time per Test Run', fontsize=12, fontweight='bold')
            plt.xlabel('Test Run', fontsize=10)
            plt.ylabel('Average Time (seconds)', fontsize=10)
            plt.grid(True, alpha=0.3)
            
            # Adicionar valores nos pontos
            for x, y in zip(avg_by_run.index, avg_by_run.values):
                plt.text(x, y, f'{y:.2f}s', ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error creating plot 2: {e}")
            plt.text(0.5, 0.5, 'Error creating plot\nAverage Execution Time per Test Run', 
                    ha='center', va='center', fontsize=10)
    
    # 3. Gráfico: Memory Usage Distribution
    plt.subplot(2, 3, 3)
    if 'memory_usage_mb' in df.columns:
        try:
            # Converter para numérico e remover NaN
            df['memory_usage_mb'] = pd.to_numeric(df['memory_usage_mb'], errors='coerce')
            memory_data = df['memory_usage_mb'].dropna()
            
            if len(memory_data) > 0:
                plt.hist(memory_data, bins=20, color='lightgreen', alpha=0.7, edgecolor='black')
                plt.title('Memory Usage Distribution', fontsize=12, fontweight='bold')
                plt.xlabel('Memory Usage (MB)', fontsize=10)
                plt.ylabel('Frequency', fontsize=10)
                plt.grid(True, alpha=0.3, axis='y')
                
                # Adicionar estatísticas
                mean_mem = memory_data.mean()
                median_mem = memory_data.median()
                plt.axvline(mean_mem, color='red', linestyle='--', linewidth=1.5, label=f'Mean: {mean_mem:.1f}MB')
                plt.axvline(median_mem, color='blue', linestyle='--', linewidth=1.5, label=f'Median: {median_mem:.1f}MB')
                plt.legend(fontsize=8)
            else:
                plt.text(0.5, 0.5, 'No memory usage data', ha='center', va='center', fontsize=10)
        except Exception as e:
            print(f"Error creating plot 3: {e}")
            plt.text(0.5, 0.5, 'Error creating plot\nMemory Usage Distribution', 
                    ha='center', va='center', fontsize=10)
    
    # 4. Gráfico: Average System Load per Test Run
    plt.subplot(2, 3, 4)
    if 'test_run' in df.columns and 'system_load' in df.columns:
        try:
            # Converter para numérico
            df['system_load'] = pd.to_numeric(df['system_load'], errors='coerce')
            avg_load_by_run = df.groupby('test_run')['system_load'].mean()
            
            colors = ['#FF9999' if load > 70 else '#66B2FF' if load > 30 else '#99FF99' 
                     for load in avg_load_by_run.values]
            
            bars = avg_load_by_run.plot(kind='bar', color=colors, alpha=0.7, ax=plt.gca())
            plt.title('Average System Load per Test Run', fontsize=12, fontweight='bold')
            plt.xlabel('Test Run', fontsize=10)
            plt.ylabel('System Load (%)', fontsize=10)
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3, axis='y')
            
            # Adicionar valores nas barras
            for i, (idx, val) in enumerate(zip(avg_load_by_run.index, avg_load_by_run.values)):
                plt.text(i, val + 1, f'{val:.1f}%', ha='center', va='bottom', fontsize=9)
            
            # Adicionar linhas de referência
            plt.axhline(y=70, color='red', linestyle='--', alpha=0.5, linewidth=1, label='High Load (70%)')
            plt.axhline(y=30, color='green', linestyle='--', alpha=0.5, linewidth=1, label='Low Load (30%)')
            plt.legend(fontsize=8)
        except Exception as e:
            print(f"Error creating plot 4: {e}")
            plt.text(0.5, 0.5, 'Error creating plot\nAverage System Load per Test Run', 
                    ha='center', va='center', fontsize=10)
    
    # 5. Gráfico: Number of Threads per Test Run
    plt.subplot(2, 3, 5)
    if 'test_run' in df.columns and 'thread_index' in df.columns:
        try:
            threads_per_run = df.groupby('test_run')['thread_index'].nunique()
            
            # Criar barras com gradiente de cor
            colors = plt.cm.Blues(np.linspace(0.4, 0.8, len(threads_per_run)))
            
            bars = plt.bar(threads_per_run.index.astype(str), threads_per_run.values, 
                          color=colors, alpha=0.7, edgecolor='black')
            plt.title('Number of Threads per Test Run', fontsize=12, fontweight='bold')
            plt.xlabel('Test Run', fontsize=10)
            plt.ylabel('Number of Threads', fontsize=10)
            plt.grid(True, alpha=0.3, axis='y')
            
            # Adicionar valores nas barras
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                        f'{int(height)}', ha='center', va='bottom', fontsize=9)
            
            # Linha de média
            avg_threads = threads_per_run.mean()
            plt.axhline(y=avg_threads, color='red', linestyle='--', linewidth=1.5, 
                       label=f'Average: {avg_threads:.1f}')
            plt.legend(fontsize=8)
        except Exception as e:
            print(f"Error creating plot 5: {e}")
            plt.text(0.5, 0.5, 'Error creating plot\nNumber of Threads per Test Run', 
                    ha='center', va='center', fontsize=10)
    
    # 6. Gráfico: Total Execution Time per Test Run
    plt.subplot(2, 3, 6)
    if 'test_run' in df.columns and 'elapsed_time' in df.columns:
        try:
            total_time_by_run = df.groupby('test_run')['elapsed_time'].sum()
            
            # Criar gráfico de área com gradiente
            plt.fill_between(total_time_by_run.index, 0, total_time_by_run.values, 
                           alpha=0.4, color='red')
            plt.plot(total_time_by_run.index, total_time_by_run.values, 
                    color='red', linewidth=2, marker='o', markersize=6)
            plt.title('Total Execution Time per Test Run', fontsize=12, fontweight='bold')
            plt.xlabel('Test Run', fontsize=10)
            plt.ylabel('Total Time (seconds)', fontsize=10)
            plt.grid(True, alpha=0.3)
            
            # Adicionar valores nos pontos
            for x, y in zip(total_time_by_run.index, total_time_by_run.values):
                plt.text(x, y, f'{y:.1f}s', ha='center', va='bottom', fontsize=9)
            
            # Calcular e mostrar crescimento
            if len(total_time_by_run) > 1:
                first = total_time_by_run.iloc[0]
                last = total_time_by_run.iloc[-1]
                growth = ((last - first) / first * 100) if first > 0 else 0
                plt.text(0.02, 0.98, f'Growth: {growth:.1f}%', 
                        transform=plt.gca().transAxes, fontsize=9,
                        verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        except Exception as e:
            print(f"Error creating plot 6: {e}")
            plt.text(0.5, 0.5, 'Error creating plot\nTotal Execution Time per Test Run', 
                    ha='center', va='center', fontsize=10)
    
    plt.tight_layout()
    
    plot_filename = f"plots/threading_selected_plots.png"
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    print(f"✓ Plot saved as: {plot_filename}")
    
    # Não mostrar a imagem diretamente
    plt.close()
    
    # Mostrar estatísticas do summary
    if os.path.exists(summary_file):
        try:
            df_summary = read_csv_safe(summary_file)
            if not df_summary.empty:
                print(f"\nThreading Summary Statistics:")
                print(df_summary.to_string(index=False))
        except Exception as e:
            print(f"Error reading summary file: {e}")

def cleanup_old_data():
    """Limpa ou corrige dados antigos se necessário"""
    results_file = "data/threading_results.csv"
    summary_file = "data/threading_summary.csv"
    
    # Limpar ambos os arquivos
    for file_path in [results_file, summary_file]:
        if os.path.exists(file_path):
            try:
                # Criar backup
                backup_file = f"{file_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                import shutil
                shutil.copy2(file_path, backup_file)
                print(f"✓ Backup created: {backup_file}")
                
                # Limpar arquivo
                with open(file_path, 'w') as f:
                    f.write('')  # Arquivo vazio
                print(f"✓ Cleaned {file_path}")
            except Exception as e:
                print(f"Error during cleanup of {file_path}: {e}")

def main():
    """Função principal"""
    IsWinOS()
    
    # Limpar dados antigos se necessário
    cleanup_old_data()
    
    logger = MultiThreadLogger()
    
    print("\n" + "="*60)
    print("MULTIPLE TEST RUNS EXPERIMENT - THREADING")
    print("="*60)
    
    # Configurações de teste
    thread_counts = [4, 8, 16]
    all_total_times = []
    
    # Executar 5 vezes
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
            
            # Calcular summary para esta execução
            logger.calculate_summary(test_run=test_run)
            
            time.sleep(1)  # Pequena pausa entre configurações
        
        all_total_times.append(run_times)
        
        print(f"\nTest Run {test_run} Results:")
        for i, count in enumerate(thread_counts):
            print(f"  {count} threads: {run_times[i]:.2f} seconds")
    
    # Adicionar linha final com médias gerais no summary
    print(f"\n{'#'*60}")
    print("FINALIZING SUMMARY FILE")
    print(f"{'#'*60}")
    
    # Ler o summary atual
    try:
        df_summary = pd.read_csv(logger.summary_file)
        
        # Calcular médias gerais
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
            
            # Adicionar ao dataframe
            df_final = pd.DataFrame([final_summary])
            df_summary = pd.concat([df_summary, df_final], ignore_index=True)
            
            # Salvar de volta
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