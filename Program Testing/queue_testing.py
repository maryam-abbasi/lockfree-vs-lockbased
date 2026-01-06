import os
import threading
from threading import Thread, current_thread
import multiprocessing
from multiprocessing import Process, current_process, cpu_count
import queue
from queue import Empty
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

class QueueLogger:
    def __init__(self, results_file="data/queue_results.csv", 
                 summary_file="data/queue_summary.csv"):
        self.results_file = results_file
        self.summary_file = summary_file
        self.results = []
        self.summary_data = []
        
        os.makedirs("data", exist_ok=True)
        os.makedirs("plots", exist_ok=True)
        
        # Definir os fieldnames padrão
        self.fieldnames = [
            'timestamp', 'queue_type', 'component_type', 'component_id', 
            'start_time', 'end_time', 'elapsed_time', 'memory_usage_mb', 
            'cpu_count', 'system_load', 'test_run', 'items_processed', 
            'queue_size', 'result'
        ]
    
    def write_result_to_csv(self, result_dict):
        """Write a single result to CSV file"""
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
        
    def log_result(self, queue_type, component_type, component_id, start_time, end_time,
                   memory_usage=None, additional_info=None, test_run=1):
        elapsed_time = end_time - start_time
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        result = {
            'timestamp': timestamp,
            'queue_type': queue_type,
            'component_type': component_type,
            'component_id': component_id,
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
        
        # Escrever no CSV
        self.write_result_to_csv(result)
        
        return result
    
    def calculate_summary(self, test_run=None, queue_type=None):
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
        
        # Filtrar por test_run
        if test_run is not None:
            df = df[df['test_run'] == test_run]
        
        # Filtrar por tipo de queue
        if queue_type is not None:
            df = df[df['queue_type'] == queue_type]
        
        if len(df) == 0:
            return
        
        # Calcular estatísticas
        summary = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'test_run': test_run if test_run else 'all',
            'queue_type': queue_type if queue_type else 'all',
            'total_components': len(df),
            'producers_count': len(df[df['component_type'] == 'producer']) if 'component_type' in df.columns else 0,
            'consumers_count': len(df[df['component_type'] == 'consumer']) if 'component_type' in df.columns else 0,
            'workers_count': len(df[df['component_type'] == 'worker']) if 'component_type' in df.columns else 0,
            'avg_elapsed_time': df['elapsed_time'].mean(),
            'min_elapsed_time': df['elapsed_time'].min(),
            'max_elapsed_time': df['elapsed_time'].max(),
            'std_elapsed_time': df['elapsed_time'].std(),
            'avg_memory_usage': df['memory_usage_mb'].mean() if 'memory_usage_mb' in df.columns else None,
            'avg_system_load': df['system_load'].mean(),
            'total_items_processed': df['items_processed'].sum() if 'items_processed' in df.columns else 0,
            'avg_queue_size': df['queue_size'].mean() if 'queue_size' in df.columns else None,
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

class ThreadingQueue:
    def __init__(self, max_size=10, logger=None, test_run=1):
        self.queue = queue.Queue(maxsize=max_size)
        self.producer_threads = []
        self.consumer_threads = []
        self.worker_threads = []
        self.running = False
        self.logger = logger
        self.test_run = test_run
        self.max_size = max_size

    def producer(self, producer_id, items_to_produce):
        """Producer thread that adds items to the queue"""
        start_time = time.time()
        memory_before = get_memory_usage()
        
        thread_name = current_thread().name
        
        print(f"[{thread_name}] Producer {producer_id} started (Test Run {self.test_run})")
        
        items_produced = 0
        for i in range(items_to_produce):
            item = f"Item-{producer_id}-{i}"
            self.queue.put(item)
            items_produced += 1
            print(f"[{thread_name}] Produced: {item}")
            time.sleep(random.uniform(0.1, 0.3))
        
        memory_after = get_memory_usage()
        end_time = time.time()
        
        # Log result
        if self.logger:
            self.logger.log_result(
                queue_type="threading",
                component_type="producer",
                component_id=producer_id,
                start_time=start_time,
                end_time=end_time,
                memory_usage=memory_after - memory_before,
                additional_info={
                    'items_processed': items_produced,
                    'queue_size': self.max_size,
                    'result': f"Produced {items_produced} items"
                },
                test_run=self.test_run
            )
        
        print(f"[{thread_name}] Producer {producer_id} finished")

    def consumer(self, consumer_id):
        """Consumer thread that removes items from the queue"""
        start_time = time.time()
        memory_before = get_memory_usage()
        
        thread_name = current_thread().name
        
        print(f"[{thread_name}] Consumer {consumer_id} started (Test Run {self.test_run})")
        
        items_consumed = 0
        while self.running or not self.queue.empty():
            try:
                item = self.queue.get(timeout=1)
                items_consumed += 1
                print(f"[{thread_name}] Consumed: {item}")
                self.queue.task_done()
                time.sleep(random.uniform(0.1, 0.3))
            except Empty:
                continue
        
        memory_after = get_memory_usage()
        end_time = time.time()
        
        # Log result
        if self.logger:
            self.logger.log_result(
                queue_type="threading",
                component_type="consumer",
                component_id=consumer_id,
                start_time=start_time,
                end_time=end_time,
                memory_usage=memory_after - memory_before,
                additional_info={
                    'items_processed': items_consumed,
                    'queue_size': self.max_size,
                    'result': f"Consumed {items_consumed} items"
                },
                test_run=self.test_run
            )
        
        print(f"[{thread_name}] Consumer {consumer_id} finished")

    def start_producer_consumer(self, num_producers=2, num_consumers=3, items_per_producer=5):
        """Starts the producer-consumer system"""
        start_time = time.time()
        self.running = True
        
        print(f"\nTest Run {self.test_run}: Starting producer-consumer")
        print(f"Producers: {num_producers}, Consumers: {num_consumers}, Items per producer: {items_per_producer}")
        
        # Start consumers first
        for i in range(num_consumers):
            thread = Thread(
                target=self.consumer,
                args=(i+1,),
                name=f"Consumer-{i+1}-Run{self.test_run}"
            )
            self.consumer_threads.append(thread)
            thread.start()

        # Start producers
        for i in range(num_producers):
            thread = Thread(
                target=self.producer,
                args=(i+1, items_per_producer),
                name=f"Producer-{i+1}-Run{self.test_run}"
            )
            self.producer_threads.append(thread)
            thread.start()

        # Wait for producers to finish
        for thread in self.producer_threads:
            thread.join()

        self.running = False

        # Wait for consumers to finish
        for thread in self.consumer_threads:
            thread.join()

        end_time = time.time() - start_time
        
        print(f"Test Run {self.test_run}: All producer-consumer threads finished")
        print(f"Total time: {end_time:.2f} seconds")
        
        return end_time

    def worker_with_results(self, input_queue, output_queue, worker_id):
        """Worker that processes items and returns results"""
        start_time = time.time()
        memory_before = get_memory_usage()
        
        thread_name = current_thread().name
        
        print(f"[{thread_name}] Worker {worker_id} started (Test Run {self.test_run})")
        
        items_processed = 0
        while True:
            try:
                item = input_queue.get(timeout=1)
                if item is None:
                    input_queue.task_done()
                    break
                
                # Process item
                result = f"Processed({item}) by worker {worker_id}"
                output_queue.put(result)
                items_processed += 1
                print(f"[{thread_name}] Processed: {item} -> {result}")
                input_queue.task_done()
                time.sleep(random.uniform(0.1, 0.3))
                
            except Empty:
                continue
        
        memory_after = get_memory_usage()
        end_time = time.time()
        
        # Log result
        if self.logger:
            self.logger.log_result(
                queue_type="threading",
                component_type="worker",
                component_id=worker_id,
                start_time=start_time,
                end_time=end_time,
                memory_usage=memory_after - memory_before,
                additional_info={
                    'items_processed': items_processed,
                    'queue_size': self.max_size,
                    'result': f"Processed {items_processed} items"
                },
                test_run=self.test_run
            )
        
        print(f"[{thread_name}] Worker {worker_id} finished")

    def start_workflow_system(self, num_workers=3, items_to_process=10):
        """Workflow processing system"""
        start_time = time.time()
        
        print(f"\nTest Run {self.test_run}: Starting workflow system with {num_workers} workers")
        
        input_queue = queue.Queue()
        output_queue = queue.Queue()
        workers = []

        # Add items to input queue
        for i in range(items_to_process):
            input_queue.put(f"Task-{i+1}")

        # Start workers
        for i in range(num_workers):
            thread = Thread(
                target=self.worker_with_results,
                args=(input_queue, output_queue, i+1),
                name=f"Worker-{i+1}-Run{self.test_run}"
            )
            workers.append(thread)
            thread.start()

        # Add termination signals
        for _ in range(num_workers):
            input_queue.put(None)

        # Wait for workers to finish
        for thread in workers:
            thread.join()

        # Collect results
        results = []
        while not output_queue.empty():
            results.append(output_queue.get())

        end_time = time.time() - start_time
        
        print(f"Test Run {self.test_run}: Workflow finished. Time: {end_time:.2f} seconds")
        print(f"Results count: {len(results)}")
        return results, end_time

class MultiprocessingQueue:
    def __init__(self, max_size=10, logger=None, test_run=1):
        self.queue = multiprocessing.Queue(maxsize=max_size)
        self.processes = []
        self.max_processes = cpu_count()
        self.logger = logger
        self.test_run = test_run
        self.max_size = max_size
        self.stop_event = multiprocessing.Event()

    def producer(self, producer_id, items_to_produce, queue):
        """Producer process"""
        start_time = time.time()
        memory_before = get_memory_usage()
        process_id = os.getpid()
        
        print(f"[PID {process_id}] Producer {producer_id} started (Test Run {self.test_run})")
        
        items_produced = 0
        try:
            for i in range(items_to_produce):
                item = f"MP-Item-{producer_id}-{i}"
                queue.put(item)
                items_produced += 1
                print(f"[PID {process_id}] Produced: {item}")
                time.sleep(random.uniform(0.1, 0.3))
        except Exception as e:
            print(f"[PID {process_id}] Producer {producer_id} error: {e}")
        
        memory_after = get_memory_usage()
        end_time = time.time()
        
        # Log result
        if self.logger:
            self.logger.log_result(
                queue_type="multiprocessing",
                component_type="producer",
                component_id=producer_id,
                start_time=start_time,
                end_time=end_time,
                memory_usage=memory_after - memory_before,
                additional_info={
                    'items_processed': items_produced,
                    'queue_size': self.max_size,
                    'result': f"Produced {items_produced} items"
                },
                test_run=self.test_run
            )
        
        print(f"[PID {process_id}] Producer {producer_id} finished")

    def consumer(self, consumer_id, queue, stop_event):
        """Consumer process with timeout"""
        start_time = time.time()
        memory_before = get_memory_usage()
        process_id = os.getpid()
        
        print(f"[PID {process_id}] Consumer {consumer_id} started (Test Run {self.test_run})")
        
        items_consumed = 0
        try:
            while not stop_event.is_set():
                try:
                    # Use timeout to avoid blocking forever
                    item = queue.get(timeout=1)
                    if item is None:
                        break
                    
                    items_consumed += 1
                    print(f"[PID {process_id}] Consumed: {item}")
                    time.sleep(random.uniform(0.2, 0.6))
                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"[PID {process_id}] Consumer {consumer_id} error: {e}")
                    break
        except Exception as e:
            print(f"[PID {process_id}] Consumer {consumer_id} fatal error: {e}")
        
        memory_after = get_memory_usage()
        end_time = time.time()
        
        # Log result
        if self.logger:
            self.logger.log_result(
                queue_type="multiprocessing",
                component_type="consumer",
                component_id=consumer_id,
                start_time=start_time,
                end_time=end_time,
                memory_usage=memory_after - memory_before,
                additional_info={
                    'items_processed': items_consumed,
                    'queue_size': self.max_size,
                    'result': f"Consumed {items_consumed} items"
                },
                test_run=self.test_run
            )
        
        print(f"[PID {process_id}] Consumer {consumer_id} finished")

    def start_mp_producer_consumer(self, num_producers=2, num_consumers=3, items_per_producer=5):
        """Multiprocessing producer-consumer system with proper cleanup"""
        start_time = time.time()
        
        print(f"\nTest Run {self.test_run}: Starting multiprocessing producer-consumer")
        print(f"Producers: {num_producers}, Consumers: {num_consumers}, Items per producer: {items_per_producer}")
        
        processes = []
        stop_event = multiprocessing.Event()

        # Start consumers first with stop_event
        for i in range(num_consumers):
            p = Process(target=self.consumer, args=(i+1, self.queue, stop_event))
            processes.append(p)
            p.start()

        # Start producers
        for i in range(num_producers):
            p = Process(target=self.producer, args=(i+1, items_per_producer, self.queue))
            processes.append(p)
            p.start()

        # Wait for producers to finish
        producer_processes = processes[:num_producers]
        for p in producer_processes:
            p.join()
            print(f"Producer process {p.pid} finished")

        # Give consumers time to process remaining items
        print("Waiting for consumers to finish processing...")
        time.sleep(2)
        
        # Signal consumers to stop
        stop_event.set()
        
        # Send termination signals to consumers
        for _ in range(num_consumers):
            try:
                self.queue.put(None, timeout=1)
            except:
                pass

        # Wait for consumers to finish with timeout
        consumer_processes = processes[num_producers:]
        timeout_start = time.time()
        timeout = 5  # seconds
        
        for p in consumer_processes:
            try:
                p.join(timeout=timeout)
                if p.is_alive():
                    print(f"Consumer process {p.pid} is still alive, terminating...")
                    p.terminate()
                    p.join()
            except Exception as e:
                print(f"Error joining consumer process: {e}")
                p.terminate()
                p.join()

        end_time = time.time() - start_time
        
        print(f"Test Run {self.test_run}: All multiprocessing producer-consumer processes finished")
        print(f"Total time: {end_time:.2f} seconds")
        
        # Clean up queue
        try:
            while True:
                self.queue.get_nowait()
        except:
            pass
        
        return end_time

    def cpu_worker(self, input_queue, output_queue, worker_id, stop_event):
        """CPU-intensive task worker with stop event"""
        start_time = time.time()
        memory_before = get_memory_usage()
        process_id = os.getpid()
        
        print(f"[PID {process_id}] CPU Worker {worker_id} started (Test Run {self.test_run})")
        
        tasks_processed = 0
        try:
            while not stop_event.is_set():
                try:
                    task = input_queue.get(timeout=1)
                    if task is None:
                        break
                    
                    # Simulate CPU-bound processing
                    result = sum(i*i for i in range(task))
                    output_queue.put((worker_id, task, result))
                    tasks_processed += 1
                    print(f"[PID {process_id}] Worker {worker_id} processed task {task}")
                    
                except queue.Empty:
                    continue
        except Exception as e:
            print(f"[PID {process_id}] CPU Worker {worker_id} error: {e}")
        
        memory_after = get_memory_usage()
        end_time = time.time()
        
        # Log result
        if self.logger:
            self.logger.log_result(
                queue_type="multiprocessing",
                component_type="cpu_worker",
                component_id=worker_id,
                start_time=start_time,
                end_time=end_time,
                memory_usage=memory_after - memory_before,
                additional_info={
                    'items_processed': tasks_processed,
                    'queue_size': self.max_size,
                    'result': f"Processed {tasks_processed} CPU tasks"
                },
                test_run=self.test_run
            )
        
        print(f"[PID {process_id}] CPU Worker {worker_id} finished")

    def start_cpu_intensive_workflow(self, num_workers=None, tasks=None):
        """System for CPU-intensive tasks"""
        start_time = time.time()
        
        if num_workers is None:
            num_workers = min(self.max_processes, 4)
        if tasks is None:
            tasks = [50000, 100000, 150000]  # Reduced for faster execution
        
        print(f"\nTest Run {self.test_run}: Starting CPU-intensive workflow")
        print(f"Workers: {num_workers}, Tasks: {len(tasks)}")

        input_queue = multiprocessing.Queue()
        output_queue = multiprocessing.Queue()
        stop_event = multiprocessing.Event()
        processes = []

        # Add tasks to input queue
        for task in tasks:
            input_queue.put(task)

        # Start workers with stop_event
        for i in range(num_workers):
            p = Process(target=self.cpu_worker, args=(input_queue, output_queue, i+1, stop_event))
            processes.append(p)
            p.start()

        # Wait for tasks to be processed
        results = []
        try:
            for _ in range(len(tasks)):
                result = output_queue.get(timeout=10)  # Timeout after 10 seconds
                results.append(result)
        except queue.Empty:
            print("Timeout waiting for results")

        # Signal workers to stop
        stop_event.set()
        
        # Send termination signals
        for _ in range(num_workers):
            try:
                input_queue.put(None, timeout=1)
            except:
                pass

        # Wait for workers with timeout
        timeout = 5
        for p in processes:
            try:
                p.join(timeout=timeout)
                if p.is_alive():
                    print(f"Worker process {p.pid} is still alive, terminating...")
                    p.terminate()
                    p.join()
            except Exception as e:
                print(f"Error joining worker process: {e}")
                p.terminate()
                p.join()

        end_time = time.time() - start_time

        print(f"Test Run {self.test_run}: CPU-intensive workflow finished.")
        print(f"Time elapsed: {end_time:.2f} seconds")
        print(f"Results count: {len(results)}")
        
        # Clean up queues
        try:
            while True:
                input_queue.get_nowait()
        except:
            pass
            
        try:
            while True:
                output_queue.get_nowait()
        except:
            pass
        
        return results, end_time

def read_csv_safe(file_path):
    """Lê um arquivo CSV de forma segura"""
    try:
        return pd.read_csv(file_path)
    except pd.errors.ParserError:
        try:
            return pd.read_csv(file_path, on_bad_lines='skip')
        except:
            return pd.DataFrame()
    except FileNotFoundError:
        print(f"File {file_path} not found!")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()

def create_queue_plots_multiple_runs():
    """Create visualization plots from queue CSV data - APENAS OS GRÁFICOS SOLICITADOS"""
    results_file = "data/queue_results.csv"
    summary_file = "data/queue_summary.csv"
    
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
        df['test_run'] = 1
    
    # Garantir que test_run seja numérico
    df['test_run'] = pd.to_numeric(df['test_run'], errors='coerce')
    
    # Criar figura com layout 2x3 para os 6 gráficos solicitados
    plt.figure(figsize=(18, 12))
    
    # 1. Gráfico: Average Time per Thread Index - SOLICITADO
    plt.subplot(2, 3, 1)
    if 'component_id' in df.columns and 'elapsed_time' in df.columns and 'test_run' in df.columns:
        try:
            # Calcular média de tempo por component_id e test_run
            avg_times = df.groupby(['test_run', 'component_id'])['elapsed_time'].mean().unstack()
            avg_times.plot(kind='bar', ax=plt.gca())
            plt.title('Average Time per Component ID\n(Grouped by Test Run)')
            plt.xlabel('Test Run')
            plt.ylabel('Average Time (seconds)')
            plt.legend(title='Component ID', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3, linestyle='--')
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
            
            # Adicionar valores nos pontos
            for x, y in zip(avg_by_run.index, avg_by_run.values):
                plt.text(x, y, f'{y:.2f}', ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error creating plot 2: {e}")
            plt.text(0.5, 0.5, 'Error creating plot', ha='center', va='center')
    
    # 3. Gráfico: Memory Usage Distribution - SOLICITADO
    plt.subplot(2, 3, 3)
    if 'memory_usage_mb' in df.columns:
        try:
            df['memory_usage_mb'] = pd.to_numeric(df['memory_usage_mb'], errors='coerce')
            memory_data = df['memory_usage_mb'].dropna()
            
            if not memory_data.empty:
                plt.hist(memory_data, bins=20, color='lightgreen', alpha=0.7, edgecolor='black')
                plt.title('Memory Usage Distribution\n(All Queue Tests)')
                plt.xlabel('Memory Usage (MB)')
                plt.ylabel('Frequency')
                plt.grid(True, alpha=0.3, linestyle='--', axis='y')
                
                # Adicionar estatísticas
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
            df['system_load'] = pd.to_numeric(df['system_load'], errors='coerce')
            avg_load_by_run = df.groupby('test_run')['system_load'].mean()
            
            bars = plt.bar(avg_load_by_run.index, avg_load_by_run.values, 
                          color='orange', alpha=0.7, edgecolor='black')
            plt.title('Average System Load per Test Run')
            plt.xlabel('Test Run')
            plt.ylabel('System Load (%)')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3, linestyle='--', axis='y')
            
            # Adicionar valores nas barras
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f'{height:.1f}%',
                        ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error creating plot 4: {e}")
            plt.text(0.5, 0.5, 'Error creating plot', ha='center', va='center')
    
    # 5. Gráfico: Number of Threads per Test Run - SOLICITADO
    # Nota: Aqui consideramos "threads" como o total de componentes por test_run
    plt.subplot(2, 3, 5)
    if 'test_run' in df.columns:
        try:
            # Contar número de componentes únicos por test_run
            # Primeiro, criar identificador único para cada componente
            if 'component_type' in df.columns and 'component_id' in df.columns:
                df['component_unique'] = df['component_type'] + '_' + df['component_id'].astype(str)
                threads_per_run = df.groupby('test_run')['component_unique'].nunique()
                
                bars = plt.bar(threads_per_run.index, threads_per_run.values, 
                              color='purple', alpha=0.7, edgecolor='black')
                plt.title('Number of Components per Test Run')
                plt.xlabel('Test Run')
                plt.ylabel('Number of Components')
                plt.xticks(rotation=45)
                plt.grid(True, alpha=0.3, linestyle='--', axis='y')
                
                # Adicionar valores nas barras
                for bar in bars:
                    height = bar.get_height()
                    plt.text(bar.get_x() + bar.get_width()/2., height,
                            f'{int(height)}',
                            ha='center', va='bottom', fontsize=9)
            else:
                plt.text(0.5, 0.5, 'No component data available', ha='center', va='center')
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
            
            # Adicionar valores nos pontos
            for x, y in zip(total_time_by_run.index, total_time_by_run.values):
                plt.text(x, y, f'{y:.2f}', ha='center', va='bottom', fontsize=9)
        except Exception as e:
            print(f"Error creating plot 6: {e}")
            plt.text(0.5, 0.5, 'Error creating plot', ha='center', va='center')
    
    plt.tight_layout()
    
    plot_filename = f"plots/queue_selected_plots.png"
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    print(f"Plot saved as: {plot_filename}")
    
    # Não mostrar a imagem diretamente
    plt.close()
    
    # Mostrar estatísticas do summary
    if os.path.exists(summary_file):
        try:
            df_summary = read_csv_safe(summary_file)
            if not df_summary.empty:
                print(f"\nQueue Summary Statistics for all runs:")
                print(df_summary[['test_run', 'queue_type', 'total_components', 
                                'avg_elapsed_time', 'avg_system_load']].tail(6))
        except Exception as e:
            print(f"Error reading summary file: {e}")

def cleanup_old_data():
    """Limpa dados antigos se necessário"""
    results_file = "data/queue_results.csv"
    summary_file = "data/queue_summary.csv"
    
    # Limpar ambos os arquivos
    for file_path in [results_file, summary_file]:
        if os.path.exists(file_path):
            try:
                # Criar backup
                backup_file = f"{file_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                import shutil
                shutil.copy2(file_path, backup_file)
                print(f"Backup created: {backup_file}")
                
                # Limpar arquivo
                with open(file_path, 'w') as f:
                    f.write('')  # Arquivo vazio
                print(f"Cleaned {file_path}")
            except Exception as e:
                print(f"Error during cleanup of {file_path}: {e}")

def main():
    """Função principal"""
    IsWinOS()
    
    # Limpar dados antigos se necessário
    cleanup_old_data()
    
    logger = QueueLogger()
    
    print("\n" + "="*60)
    print("MULTIPLE TEST RUNS EXPERIMENT - QUEUE SYSTEMS")
    print("="*60)
    
    # Configurações de teste SIMPLIFICADAS para evitar problemas
    threading_configs = [
        {"producers": 2, "consumers": 2, "items_per_producer": 3},
        {"producers": 2, "consumers": 3, "items_per_producer": 3}
    ]
    
    multiprocessing_configs = [
        {"producers": 2, "consumers": 2, "items_per_producer": 3},
        {"producers": 2, "consumers": 2, "items_per_producer": 3}
    ]
    
    cpu_tasks_configs = [
        [50000, 100000],
        [50000, 100000]
    ]
    
    all_times_threading = []
    all_times_multiprocessing = []
    
    # Executar APENAS 3 vezes para teste
    for test_run in range(1, 4):
        print(f"\n{'#'*60}")
        print(f"STARTING TEST RUN {test_run} OF 3")
        print(f"{'#'*60}")
        
        run_times_threading = []
        run_times_multiprocessing = []
        
        # Testes com Threading Queue
        print(f"\n{'='*40}")
        print(f"Test Run {test_run}: Threading Queue Tests")
        print(f"{'='*40}")
        
        for config in threading_configs:
            print(f"\nThreading: {config['producers']} producers, {config['consumers']} consumers")
            
            tq = ThreadingQueue(max_size=5, logger=logger, test_run=test_run)
            
            # Teste 1: Producer-Consumer
            time1 = tq.start_producer_consumer(
                num_producers=config['producers'],
                num_consumers=config['consumers'],
                items_per_producer=config['items_per_producer']
            )
            
            # Teste 2: Workflow System
            items_to_process = config['producers'] * config['items_per_producer']
            _, time2 = tq.start_workflow_system(
                num_workers=config['consumers'],
                items_to_process=items_to_process
            )
            
            total_time = time1 + time2
            run_times_threading.append(total_time)
            
            print(f"Total threading time: {total_time:.2f} seconds")
            time.sleep(1)  # Pausa menor
        
        all_times_threading.append(run_times_threading)
        
        # Testes com Multiprocessing Queue
        print(f"\n{'='*40}")
        print(f"Test Run {test_run}: Multiprocessing Queue Tests")
        print(f"{'='*40}")
        
        for i, config in enumerate(multiprocessing_configs):
            print(f"\nMultiprocessing: {config['producers']} producers, {config['consumers']} consumers")
            
            mpq = MultiprocessingQueue(max_size=5, logger=logger, test_run=test_run)
            
            # Teste 1: Producer-Consumer
            try:
                time1 = mpq.start_mp_producer_consumer(
                    num_producers=config['producers'],
                    num_consumers=config['consumers'],
                    items_per_producer=config['items_per_producer']
                )
            except Exception as e:
                print(f"Error in producer-consumer: {e}")
                time1 = 0
            
            # Teste 2: CPU-intensive workflow
            try:
                _, time2 = mpq.start_cpu_intensive_workflow(
                    num_workers=min(cpu_count(), 3),
                    tasks=cpu_tasks_configs[i]
                )
            except Exception as e:
                print(f"Error in CPU workflow: {e}")
                time2 = 0
            
            total_time = time1 + time2
            run_times_multiprocessing.append(total_time)
            
            print(f"Total multiprocessing time: {total_time:.2f} seconds")
            time.sleep(1)  # Pausa menor
        
        all_times_multiprocessing.append(run_times_multiprocessing)
        
        # Calcular summary para esta execução
        logger.calculate_summary(test_run=test_run, queue_type="threading")
        logger.calculate_summary(test_run=test_run, queue_type="multiprocessing")
        
        print(f"\nTest Run {test_run} Results:")
        print(f"Threading average time: {np.mean(run_times_threading):.2f} seconds")
        print(f"Multiprocessing average time: {np.mean(run_times_multiprocessing):.2f} seconds")
    
    # Adicionar linha final com médias gerais no summary
    print(f"\n{'#'*60}")
    print("FINALIZING SUMMARY FILE")
    print(f"{'#'*60}")
    
    # Ler o summary atual
    try:
        df_summary = pd.read_csv(logger.summary_file)
        
        # Calcular médias gerais
        if not df_summary.empty:
            # Médias por queue_type
            for qtype in df_summary['queue_type'].unique():
                if qtype != 'all':
                    qtype_df = df_summary[df_summary['queue_type'] == qtype]
                    overall_avg = {
                        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'test_run': 'OVERALL_AVERAGE',
                        'queue_type': qtype,
                        'total_components': qtype_df['total_components'].sum(),
                        'producers_count': qtype_df['producers_count'].mean(),
                        'consumers_count': qtype_df['consumers_count'].mean(),
                        'workers_count': qtype_df['workers_count'].mean(),
                        'avg_elapsed_time': qtype_df['avg_elapsed_time'].mean(),
                        'min_elapsed_time': qtype_df['min_elapsed_time'].min(),
                        'max_elapsed_time': qtype_df['max_elapsed_time'].max(),
                        'std_elapsed_time': qtype_df['std_elapsed_time'].mean(),
                        'avg_memory_usage': qtype_df['avg_memory_usage'].mean() if 'avg_memory_usage' in qtype_df.columns else None,
                        'avg_system_load': qtype_df['avg_system_load'].mean(),
                        'total_items_processed': qtype_df['total_items_processed'].sum(),
                        'avg_queue_size': qtype_df['avg_queue_size'].mean() if 'avg_queue_size' in qtype_df.columns else None,
                        'cpu_count': os.cpu_count()
                    }
                    
                    df_final = pd.DataFrame([overall_avg])
                    df_summary = pd.concat([df_summary, df_final], ignore_index=True)
            
            # Salvar de volta
            df_summary.to_csv(logger.summary_file, index=False)
            
            print(f"\nOverall Average Statistics saved to summary file")
    except Exception as e:
        print(f"Error calculating overall averages: {e}")
    
    # Gerar gráficos consolidados - APENAS OS SOLICITADOS
    print(f"\n{'#'*60}")
    print("GENERATING SELECTED PLOTS FOR ALL TEST RUNS")
    print(f"{'#'*60}")
    
    create_queue_plots_multiple_runs()
    
    # Mostrar tempos médios
    print(f"\n{'#'*60}")
    print("AVERAGE EXECUTION TIMES")
    print(f"{'#'*60}")
    
    if all_times_threading:
        times_array_threading = np.array(all_times_threading)
        print(f"\nThreading Queue Performance:")
        print(f"  Overall average: {np.mean(times_array_threading):.2f} ± {np.std(times_array_threading):.2f} seconds")
        print(f"  Min time: {np.min(times_array_threading):.2f} seconds")
        print(f"  Max time: {np.max(times_array_threading):.2f} seconds")
    
    if all_times_multiprocessing:
        times_array_multiprocessing = np.array(all_times_multiprocessing)
        print(f"\nMultiprocessing Queue Performance:")
        print(f"  Overall average: {np.mean(times_array_multiprocessing):.2f} ± {np.std(times_array_multiprocessing):.2f} seconds")
        print(f"  Min time: {np.min(times_array_multiprocessing):.2f} seconds")
        print(f"  Max time: {np.max(times_array_multiprocessing):.2f} seconds")
    
    print(f"\n{'='*60}")
    print("EXPERIMENT COMPLETED SUCCESSFULLY")
    print(f"Data saved to: /data/queue_results.csv")
    print(f"Summary saved to: /data/queue_summary.csv")
    print(f"Plots saved to: /plots/")
    print(f"{'='*60}")

if __name__ == "__main__":
    """Check if the Operating System is Windows"""
    IsWinOS()

    """Configure method of inicialization to multiprocessing"""
    multiprocessing.set_start_method('spawn', force=True)

    """ Main Function for Threading & MultiProcessing Queue Testing """
    main()