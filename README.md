# AdvancedTopicsFinal

## Distributed System Simulation 

### **Programming language used:**

- Python (V. 3.11.4)

### **Dependencies:**

- SimPy
  - Install with: *pip install simpy*

### **Usage:**

**Step 1:** Firstly, open the "system_conditions.json" file and alter the 6 system conditions and simulation time to your specificed values. Note the ranges of the values:

* Simulation time (sim_time_seconds): number > 0.

* Number of nodes (number_of_nodes): whole number >= 2.

* Task lengths (node_task_lengths): number > 0.

* CPU clock speeds (node_cpu_speeds): number > 0.

* Predicted error rate (predicted_error_rate): number between 0 and 100 inclusive.

* Network delay (network_delay): number > 0.

* Rollback cost (rollback_cost): number > 0.

Additionally, each index in the *node_task_lengths* and *node_cpu_speeds* lists corresponds to a node, e.g., index 0 is for Node 0, index 1 is for Node 1 etc.


**Step 2:** Following this, traverse to your terminal and enter "*Python ds_sim.py*" to run the distributed system simulation.


**Step 3:** Observe the outputs displayed in the terminal.
