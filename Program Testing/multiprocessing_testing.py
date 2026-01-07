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
        
        # Create directories if they don't exist
        os.makedirs("data", exist_ok=True)
        os.makedirs("plots", exist_ok=True)
        
        # Definir os fieldnames MESMOS do threading
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
        
        # Convert results to DataFrame for analysis
        df = pd.DataFrame(self.results)
        
        # Se especificar test_run, calcula apenas para essa execução
        if test_run is not None:
            df = df[df['test_run'] == test_run]
        
        if len(df) == 0:
            return
        
        # Calculate summary statistics - MESMAS métricas do threading
        summary = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_tests': len(df),
            'avg_elapsed_time': round(df['elapsed_time'].mean(), 4),  # ARREDONDADO 4 casas
            'min_elapsed_time': round(df['elapsed_time'].min(), 4),   # ARREDONDADO 4 casas
            'max_elapsed_time': round(df['elapsed_time'].max(), 4),   # ARREDONDADO 4 casas
            'std_elapsed_time': round(df['elapsed_time'].std(), 4),   # ARREDONDADO 4 casas
            'avg_memory_usage': round(df['memory_usage_mb'].mean(), 4) if 'memory_usage_mb' in df.columns else None,  # ARREDONDADO
            'avg_system_load': round(df['system_load'].mean(), 4),    # ARREDONDADO 4 casas
            'cpu_count': cpu_count(),
            'total_threads': df['thread_index'].nunique(),
            'test_run': test_run if test_run else 'all'
        }
        
        self.summary_data.append(summary)
        
        # Create or update summary CSV file
        df_summary = pd.DataFrame(self.summary_data)
        
        # Load existing data if file exists
        if os.path.isfile(self.summary_file):
            try:
                existing_df = pd.read_csv(self.summary_file)
                df_summary = pd.concat([existing_df, df_summary], ignore_index=True)
            except Exception as e:
                print(f"Warning: Could not read existing summary file: {e}")
        
        # Save to CSV file
        df_summary.to_csv(self.summary_file, index=False)
        
        return summary
    
    def calculate_final_summary(self):
        """Calcula a média das 5 execuções - MESMO método do threading"""
        if not self.results:
            return
        
        df = pd.DataFrame(self.results)
        
        # Obter número único de test runs
        test_runs = df['test_run'].unique()
        
        if len(test_runs) == 0:
            return
        
        # Calcular médias por test_run - MESMAS métricas
        run_summaries = []
        for run in test_runs:
            run_df = df[df['test_run'] == run]
            if len(run_df) > 0:
                summary = {
                    'test_run': run,
                    'avg_elapsed_time': round(run_df['elapsed_time'].mean(), 4),  # ARREDONDADO
                    'avg_memory_usage': round(run_df['memory_usage_mb'].mean(), 4) if 'memory_usage_mb' in run_df.columns else None,  # ARREDONDADO
                    'avg_system_load': round(run_df['system_load'].mean(), 4),  # ARREDONDADO
                    'total_threads': run_df['thread_index'].nunique()
                }
                run_summaries.append(summary)
        
        # Calcular médias finais - MESMAS métricas
        if run_summaries:
            df_runs = pd.DataFrame(run_summaries)
            final_summary = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'total_test_runs': len(test_runs),
                'final_avg_elapsed_time': round(df_runs['avg_elapsed_time'].mean(), 4),  # ARREDONDADO
                'final_min_elapsed_time': round(df_runs['avg_elapsed_time'].min(), 4),   # ARREDONDADO
                'final_max_elapsed_time': round(df_runs['avg_elapsed_time'].max(), 4),   # ARREDONDADO
                'final_std_elapsed_time': round(df_runs['avg_elapsed_time'].std(), 4),   # ARREDONDADO
                'final_avg_memory_usage': round(df_runs['avg_memory_usage'].mean(), 4) if 'avg_memory_usage' in df_runs.columns else None,  # ARREDONDADO
                'final_avg_system_load': round(df_runs['avg_system_load'].mean(), 4),  # ARREDONDADO
                'avg_threads_per_run': round(df_runs['total_threads'].mean(), 4)  # ARREDONDADO
            }
            
            # Salvar summary final
            final_file = "data/multiprocessing_final_summary.csv"
            final_df = pd.DataFrame([final_summary])
            
            if os.path.isfile(final_file):
                try:
                    existing_df = pd.read_csv(final_file)
                    final_df = pd.concat([existing_df, final_df], ignore_index=True)
                except Exception as e:
                    print(f"Warning: Could not read existing final summary file: {e}")
            
            final_df.to_csv(final_file, index=False)
            
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
        # Primeiro tentar ler normalmente
        df = pd.read_csv(file_path)
        
        # Arredondar colunas numéricas para 4 casas decimais
        for col in df.select_dtypes(include=[np.number]).columns:
            if col not in ['test_run', 'thread_index', 'cpu_count', 'rand_max', 'result', 'calculation_size']:
                df[col] = df[col].round(4)
        
        return df
    except pd.errors.ParserError as e:
        print(f"Warning: Parser error reading {file_path}: {e}")
        print("Trying to read with error handling...")
        
        # Tentar ler ignorando linhas problemáticas
        try:
            df = pd.read_csv(file_path, on_bad_lines='skip')
            print(f"Successfully read {len(df)} rows (some rows may have been skipped)")
            
            # Arredondar colunas numéricas para 4 casas decimais
            for col in df.select_dtypes(include=[np.number]).columns:
                if col not in ['test_run', 'thread_index', 'cpu_count', 'rand_max', 'result', 'calculation_size']:
                    df[col] = df[col].round(4)
            
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
                    
                    # Arredondar colunas numéricas para 4 casas decimais
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
    """Create visualization plots from CSV data - APENAS OS GRÁFICOS SOLICITADOS"""
    results_file = "data/multiprocessing_results.csv"
    summary_file = "data/multiprocessing_summary.csv"
    
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
    
    # Criar figura com 2 linhas e 3 colunas (6 gráficos)
    plt.figure(figsize=(18, 12))
    
    # Configurar formatação de 2 casas decimais para todos os eixos
    def format_two_decimals(x, pos):
        return f'{x:.2f}'
    
    # 1. Gráfico: Average Time per Process Index - SOLICITADO
    plt.subplot(2, 3, 1)
    if 'thread_index' in df.columns and 'elapsed_time' in df.columns and 'test_run' in df.columns:
        try:
            # Calcular médias por thread_index e test_run
            avg_times = df.groupby(['test_run', 'thread_index'])['elapsed_time'].mean().unstack()
            bars = avg_times.plot(kind='bar', ax=plt.gca())
            plt.title('Average Time per Process Index\n(Grouped by Test Run)')
            plt.xlabel('Test Run')
            plt.ylabel('Average Time (seconds)')
            plt.legend(title='Process Index', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3, linestyle='--')
            
            # Formatar eixo Y para 2 casas decimais
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(format_two_decimals))
            
            # Adicionar valores formatados nas barras (2 casas decimais)
            for container in bars.containers:
                for bar in container:
                    height = bar.get_height()
                    if not np.isnan(height):
                        plt.text(bar.get_x() + bar.get_width()/2., height,
                                f'{height:.2f}', ha='center', va='bottom', fontsize=8)
        except Exception as e:
            print(f"Error creating plot 1: {e}")
            plt.text(0.5, 0.5, 'Error creating plot', ha='center', va='center')
    
    # 2. Gráfico: Average Execution Time per Test Run - SOLICITADO
    plt.subplot(2, 3, 2)
    if 'test_run' in df.columns and 'elapsed_time' in df.columns:
        try:
            avg_by_run = df.groupby('test_run')['elapsed_time'].mean()
            avg_by_run.plot(kind='line', marker='o', color='blue', linewidth=2, markersize=8)
            plt.title('Average Execution Time per Test Run')
            plt.xlabel('Test Run')
            plt.ylabel('Average Time (seconds)')
            plt.grid(True, alpha=0.3, linestyle='--')
            
            # Formatar eixo Y para 2 casas decimais
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(format_two_decimals))
            
            # Adicionar valores nos pontos (2 casas decimais)
            for x, y in zip(avg_by_run.index, avg_by_run.values):
                plt.text(x, y, f'{y:.2f}', ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error creating plot 2: {e}")
            plt.text(0.5, 0.5, 'Error creating plot', ha='center', va='center')
    
    # 3. Gráfico: Memory Usage Distribution - SOLICITADO
    plt.subplot(2, 3, 3)
    if 'memory_usage_mb' in df.columns:
        try:
            # Converter para numérico e remover NaN
            df['memory_usage_mb'] = pd.to_numeric(df['memory_usage_mb'], errors='coerce')
            memory_data = df['memory_usage_mb'].dropna()
            
            if not memory_data.empty:
                plt.hist(memory_data, bins=20, color='lightgreen', alpha=0.7, edgecolor='black')
                plt.title('Memory Usage Distribution\n(All Test Runs)')
                plt.xlabel('Memory Usage (MB)')
                plt.ylabel('Frequency')
                plt.grid(True, alpha=0.3, linestyle='--', axis='y')
                
                # Formatar eixo X para 2 casas decimais
                plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(format_two_decimals))
                
                # Adicionar estatísticas (2 casas decimais)
                mean_mem = memory_data.mean()
                median_mem = memory_data.median()
                plt.axvline(mean_mem, color='red', linestyle='--', linewidth=1, label=f'Mean: {mean_mem:.2f} MB')
                plt.axvline(median_mem, color='blue', linestyle='--', linewidth=1, label=f'Median: {median_mem:.2f} MB')
                plt.legend(fontsize='small')
            else:
                plt.text(0.5, 0.5, 'No memory data available', ha='center', va='center')
        except Exception as e:
            print(f"Error creating plot 3: {e}")
            plt.text(0.5, 0.5, 'Error creating plot', ha='center', va='center')
    
    # 4. Gráfico: Average System Load per Test Run - SOLICITADO
    plt.subplot(2, 3, 4)
    if 'test_run' in df.columns and 'system_load' in df.columns:
        try:
            # Converter para numérico
            df['system_load'] = pd.to_numeric(df['system_load'], errors='coerce')
            avg_load_by_run = df.groupby('test_run')['system_load'].mean()
            
            bars = plt.bar(avg_load_by_run.index, avg_load_by_run.values, 
                          color='orange', alpha=0.7, edgecolor='black')
            plt.title('Average System Load per Test Run')
            plt.xlabel('Test Run')
            plt.ylabel('System Load (%)')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3, linestyle='--', axis='y')
            
            # Formatar eixo Y para 1 casa decimal (porcentagem)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.1f}'))
            
            # Adicionar valores nas barras (1 casa decimal para porcentagem)
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f'{height:.1f}%',
                        ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error creating plot 4: {e}")
            plt.text(0.5, 0.5, 'Error creating plot', ha='center', va='center')
    
    # 5. Gráfico: Number of Processes per Test Run - SOLICITADO
    plt.subplot(2, 3, 5)
    if 'test_run' in df.columns and 'thread_index' in df.columns:
        try:
            processes_per_run = df.groupby('test_run')['thread_index'].nunique()
            
            bars = plt.bar(processes_per_run.index, processes_per_run.values, 
                          color='purple', alpha=0.7, edgecolor='black')
            plt.title('Number of Processes per Test Run')
            plt.xlabel('Test Run')
            plt.ylabel('Number of Processes')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3, linestyle='--', axis='y')
            
            # Formatar eixo Y para inteiros (não precisa de casas decimais)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x)}'))
            
            # Adicionar valores nas barras (inteiros)
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height)}',
                        ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error creating plot 5: {e}")
            plt.text(0.5, 0.5, 'Error creating plot', ha='center', va='center')
    
    # 6. Gráfico: Total Execution Time per Test Run - SOLICITADO
    plt.subplot(2, 3, 6)
    if 'test_run' in df.columns and 'elapsed_time' in df.columns:
        try:
            total_time_by_run = df.groupby('test_run')['elapsed_time'].sum()
            
            plt.fill_between(total_time_by_run.index, 0, total_time_by_run.values, 
                            color='red', alpha=0.3)
            plt.plot(total_time_by_run.index, total_time_by_run.values, 
                    color='red', linewidth=2, marker='s', markersize=8)
            plt.title('Total Execution Time per Test Run')
            plt.xlabel('Test Run')
            plt.ylabel('Total Time (seconds)')
            plt.grid(True, alpha=0.3, linestyle='--')
            
            # Formatar eixo Y para 2 casas decimais
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(format_two_decimals))
            
            # Adicionar valores nos pontos (2 casas decimais)
            for x, y in zip(total_time_by_run.index, total_time_by_run.values):
                plt.text(x, y, f'{y:.2f}', ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error creating plot 6: {e}")
            plt.text(0.5, 0.5, 'Error creating plot', ha='center', va='center')
    
    plt.tight_layout()
    
    plot_filename = f"plots/multiprocessing_selected_plots.png"
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    print(f"Plot saved as: {plot_filename}")
    
    # Não mostrar a imagem diretamente
    plt.close()
    
    # Mostrar estatísticas resumidas
    if os.path.exists(summary_file):
        try:
            df_summary = read_csv_safe(summary_file)
            if not df_summary.empty:
                print(f"\nMultiprocessing Summary Statistics for all runs:")
                print(df_summary.tail(min(5, len(df_summary))))
        except Exception as e:
            print(f"Error reading summary file: {e}")

def cleanup_old_data():
    """Limpa ou corrige dados antigos se necessário"""
    results_file = "data/multiprocessing_results.csv"
    
    if os.path.exists(results_file):
        try:
            # Tentar ler o arquivo existente
            with open(results_file, 'r') as f:
                content = f.read()
            
            # Verificar se o arquivo tem linhas inconsistentes
            lines = content.strip().split('\n')
            if len(lines) > 0:
                # Contar colunas no cabeçalho
                header_cols = len(lines[0].split(','))
                
                # Verificar linhas inconsistentes
                bad_lines = []
                for i, line in enumerate(lines[1:], 1):  # Começar da linha 1 (após cabeçalho)
                    if len(line.split(',')) != header_cols:
                        bad_lines.append(i)
                
                if bad_lines:
                    print(f"Warning: Found {len(bad_lines)} inconsistent lines in {results_file}")
                    print("Creating backup and starting fresh file...")
                    
                    # Criar backup
                    backup_file = f"{results_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    import shutil
                    shutil.copy2(results_file, backup_file)
                    print(f"Backup created: {backup_file}")
                    
                    # Limpar arquivo (manter apenas cabeçalho)
                    with open(results_file, 'w') as f:
                        f.write(lines[0] + '\n')
                    print(f"Cleaned {results_file}, keeping only header")
        except Exception as e:
            print(f"Error during cleanup: {e}")

def main():
    """Função principal para evitar problemas de pickling"""
    IsWinOS()
    
    # Limpar dados antigos se necessário
    cleanup_old_data()
    
    # Inicializar logger no processo principal
    logger = MultiProcessingLogger()
    
    print("\n" + "="*60)
    print("MULTIPLE TEST RUNS EXPERIMENT - MULTIPROCESSING")
    print("="*60)
    
    # MESMAS contagens de threads/processos que o threading
    process_counts = [4, 8, 16]
    all_total_times = []
    
    # Executar 5 vezes - MESMO que threading
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
            
            # Calcular summary para esta execução - MESMO (após processos terminarem)
            # Carregar dados do CSV para calcular summary
            try:
                # Ler dados atualizados do CSV
                df = pd.read_csv("data/multiprocessing_results.csv")
                if not df.empty:
                    # Filtrar apenas dados deste test_run
                    run_df = df[df['test_run'] == test_run]
                    if len(run_df) > 0:
                        # Calcular summary manualmente
                        summary = {
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'total_tests': len(run_df),
                            'avg_elapsed_time': round(run_df['elapsed_time'].mean(), 4),  # ARREDONDADO
                            'min_elapsed_time': round(run_df['elapsed_time'].min(), 4),   # ARREDONDADO
                            'max_elapsed_time': round(run_df['elapsed_time'].max(), 4),   # ARREDONDADO
                            'std_elapsed_time': round(run_df['elapsed_time'].std(), 4),   # ARREDONDADO
                            'avg_memory_usage': round(run_df['memory_usage_mb'].mean(), 4) if 'memory_usage_mb' in run_df.columns else None,  # ARREDONDADO
                            'avg_system_load': round(run_df['system_load'].mean(), 4),    # ARREDONDADO
                            'cpu_count': cpu_count(),
                            'total_threads': run_df['thread_index'].nunique(),
                            'test_run': test_run
                        }
                        
                        # Adicionar ao logger
                        logger.summary_data.append(summary)
                        
                        # Salvar summary
                        df_summary = pd.DataFrame(logger.summary_data)
                        df_summary.to_csv(logger.summary_file, index=False)
            except Exception as e:
                print(f"Warning: Could not calculate summary for run {test_run}: {e}")
            
            time.sleep(1)  # Pequena pausa entre configurações - MESMO
        
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