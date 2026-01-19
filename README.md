# Comparing Concurrency Models in Python: Threading vs Multiprocessing

##  Overview
This repository presents a systematic and empirical evaluation of concurrency and synchronization models in Python, focusing on threading, multiprocessing, queues, locks, and pool-based executors under both CPU-bound and I/O-bound workloads.
The study analyzes how Python’s Global Interpreter Lock (GIL), synchronization primitives, and inter-process communication mechanisms impact performance, scalability, correctness, and resource usage across different hardware platforms.
The project was developed as an academic experimental benchmark and is designed to be reproducible, configurable, and extensible for further research in concurrent systems.

---

##  Key Contributions
- Comparative benchmarking of Threading vs Multiprocessing under controlled workloads
- Evaluation of Queue-based producer–consumer models (threading and multiprocessing)
- Analysis of race conditions, locks, semaphores, and condition variables
- Performance comparison between ThreadPoolExecutor and ProcessPoolExecutor
- Cross-hardware evaluation on three 16-logical-core machines
- Unified metric logging with tables (Pandas) and plots (Matplotlib)

---

##  Experimental Platforms
All experiments were executed on three heterogeneous systems:
- PAIVA-DESKTOP – AMD Ryzen 7 7700, 32 GB DDR5, Windows 11
- PAIVA-LAPTOP – AMD Ryzen 7 5800H, 16 GB DDR4, Windows 10
- FELIPE-LAPTOP – AMD Ryzen 7 5700U, 12 GB DDR4, Windows 11
Each platform provides 16 logical CPU cores, enabling fair scalability comparisons.

---

##  Workload Characterization
The benchmark suite includes two main workload categories:

###  CPU-Bound Tasks
- Large numeric summations and square-root computations
- Designed to stress the GIL and evaluate true parallelism via multiprocessing

### I/O-Bound Tasks
- Simulated latency using time.sleep
- Models real-world I/O scenarios such as network or disk access

---

##  Concurrency Models Evaluated
- Threading (threading.Thread)
- Multiprocessing (multiprocessing.Process)
- ThreadPoolExecutor
- ProcessPoolExecutor
- Threading Queue (queue.Queue)
- Multiprocessing Queue (multiprocessing.Queue)
- Synchronization primitives:
  - Lock / RLock
  - Semaphore
  - Condition Variables
 
---

##  Metrics Collected

###  Performance Plots
- Average execution time per thread
- Average execution time per test run
- Memory usage distribution
- Average system load
- Number of active threads or processes
- Total execution time

---

###  Summary Tables
- Total tests / tasks executed
- Producers, consumers, and workers
- Queue sizes and processed items
- Success rate (data integrity)
- Elapsed time
- Memory usage
- System load
- CPU core count

---

##  Methodology
![MethodologyFinalVersion.drawio.png](https://github.com/maryam-abbasi/lockfree-vs-lockbased/blob/main/MethodologyFinalVersion.drawio.png)

---

##  Results Summary
Key findings from the experimental analysis include:
- Race conditions persist regardless of hardware power, achieving only ~80% success without synchronization
- Multiprocessing and ProcessPoolExecutor significantly outperform threading in CPU-bound workloads
- Threading performs competitively in I/O-bound scenarios where the GIL is released
- Queues with multiprocessing reduce execution time by ~10% compared to threading queues
- ProcessPoolExecutor offers the best balance between performance and memory stability

---

##  Requirements
- Python 3.8+
- numpy
- pandas
- matplotlib
- psutil
(All dependencies are standard scientific Python libraries.)

---

##  Usage
Clone the repository and run the main benchmarking script:
```
  python main.py
```
The workload size, number of workers, and concurrency model can be configured directly in the source files.

---

##  Limitations
- All evaluated mechanisms remain lock-based at the Python interpreter level
- True lock-free CPU-bound execution is not achievable due to the Global Interpreter Lock (GIL)
- Results are specific to shared-memory single-node systems

---

##  Future Work
- Evaluation of Per-Interpreter GIL (Python 3.12+)
- Distributed execution with mpi4py or PyPVM
- Integration of asyncio for cooperative multitasking comparison
- Hybrid scheduling and adaptive workload models

---

##  Authors
- Felipe Campelo Sabbado
  Escola Superior de Tecnologia e Gestão – IPSantarém

- Rodrigo Paiva Calado
  Escola Superior de Tecnologia e Gestão – IPSantarém

---

##  Keywords
Concurrency · Threading · Multiprocessing · GIL · Queue · Locks · ProcessPool · ThreadPool · Benchmarking
