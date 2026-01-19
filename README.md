# Comparing Lock-Based vs Lock-Free Data Structures

##  Project Overview
This project studies the performance and scalability of **lock-based** and **lock-free** data structures in concurrent environments.  
The main goal is to determine **which approach is faster and more scalable** under different workloads and access patterns.

##  Research Question
Are lock-free data structures faster and more scalable than lock-based structures?

##  Background
Concurrent programs often need to safely share data among multiple threads.  
Two main strategies exist:
- **Lock-based structures:** use mutexes or critical sections to protect shared data.
- **Lock-free structures:** use atomic operations and memory consistency models to avoid locks and reduce contention.

Understanding their trade-offs is key to designing high-performance concurrent systems.

[!img](MethodologyFinalVersion.drawio.png)
---

##  Step-by-Step Plan

### **Step 1 – Research (Weeks 1–2)**
- Learn about locks, mutexes, and lock-free algorithms.
- Read 2–3 academic papers comparing the two approaches.

### **Step 2 – Lock-Based Implementation (Weeks 3–4)**
- Implement a **queue** using locks (e.g., `std::mutex` in C++).
- Run multithreaded enqueue/dequeue operations.
- Measure throughput and contention.

### **Step 3 – Lock-Free Implementation (Weeks 5–6)**
- Implement a **lock-free queue** using atomic operations (e.g., CAS).
- Run the same benchmark as for the lock-based queue.

### **Step 4 – Scalability Testing (Weeks 7–8)**
- Test both implementations with **2, 4, 8, and 16 threads**.
- Measure throughput and latency.
- Plot graphs showing scalability.

### **Step 5 – Different Access Patterns (Weeks 9–10)**
- Simulate **high**, **low**, and **mixed contention** workloads.
- Record all results and observations.

### **Step 6 – Write the Paper (Weeks 11–14)**
- **Introduction:** Explain lock-based vs lock-free design.
- **Methodology:** Describe your implementations.
- **Results:** Present throughput and scalability data.
- **Discussion:** Analyze trade-offs and limitations.
- **Conclusion:** State when each approach is preferable.

---

##  Technologies and Tools
- **Language:** python
- **Concurrency library:** 
- **Benchmarking:** custom timer or Google Benchmark
- **Visualization:** Python (Matplotlib) or Excel for plotting graphs

---

