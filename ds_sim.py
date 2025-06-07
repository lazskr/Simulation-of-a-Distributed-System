import simpy 
import random 
import sys 
import json  

class Nodes:  
    
    def __init__(self, node_identifier, task_length, cpu_speed, predicted_error_rate, network_delay, rollback_cost, num_nodes, env, start_nodes):
        # User inputs (system conditions)
        self.predicted_error_rate = predicted_error_rate
        self.network_delay = network_delay
        self.rollback_cost = rollback_cost
        self.task_length = task_length 
        self.calculation_time = task_length / cpu_speed
        self.num_nodes = num_nodes - 1
        
        # Other variables 
        self.node_identifier = node_identifier
        # Current task to complete for node 
        self.task = 0
        # Dictionary in dictionary. task : {Node : Node}
        self.expected_values = {} 
        # Dictionary in dictionary. task : {Node : Node}
        self.received_values = {} 
        # Queue for node receiving values during rollback 
        self.queue = {} 
        self.num_rollbacks = 0 
        self.num_data_shared = 0 
        self.num_expected_values_checked = 0 
        self.is_rollback = False 
        
        self.env = env
        self.start_nodes = start_nodes
        # To start all nodes at the same time 
        self.calculate_process = self.env.process(self.run_calculate()) 
        
        # New variables added (post initial design)
        self.last_rb_checkpoint = None 
        self.other_nodes_rb_checkpoints = [] 
        self.if_rollback_in_rollback = False 
        # Number of checkpoints wasted per node 
        self.total_tasks_wasted = 0
    
    # Creates the lists which the nodes will use to keep track of each other's most recent rollback checkpoint 
    def create_rollback_lists(self):
        num = self.num_nodes+1
        for _ in range(num):
            self.other_nodes_rb_checkpoints.append(None)
     
    # Assigns variable to store all the other nodes 
    def store_other_nodes(self, received_nodes): 
        self.other_nodes = received_nodes 
        
    # Starts the calculate() function 
    def run_calculate(self):
        yield self.start_nodes
        self.running_calculate = self.env.process(self.calculate())
    
    # Receive_value() calls this function to determine if the node exists in both
    # the expected and received value dictionaries for a task     
    def check_dictionaries(self, task, node, dictionaries): 
        count = 0 
        for cur_dict in dictionaries:
            if task in cur_dict and node in cur_dict[task]:
                count += 1  
        if count == 2: 
            return True 
        return False 
    
    # Function which determines if expected value was true 
    def check_expected_value(self):
        rand_num = random.uniform(0, 100)
        if (rand_num < self.predicted_error_rate):
            return False #e.g., if 10 < 11.7 => False (expected value was wrong)
        return True 
    
    # Putting queue dictionary into received_values dictionary 
    def merge_dictionaries(self):
        for task, node in self.queue.items(): 
            if task in self.received_values:
                for inner_key, inner_val in node.items():
                    if inner_key in self.received_values[task]:
                        continue
                    else:
                        self.received_values[task][inner_key] = inner_val
            else: 
                self.received_values[task] = node  
            
    # Process which performs calculations 
    def calculate(self):
        while True:
            try:        
                yield self.env.timeout(self.calculation_time)
                # Creating new task dictionary for task 
                if self.task not in self.received_values:
                    self.received_values[self.task] = {}  
                # Creating new expected value dictionary for task
                if self.task not in self.expected_values:
                    self.expected_values[self.task] = {}
                # Check which values have been received and if they havent, we use an expected value
                for node in self.other_nodes:
                    if node.node_identifier not in self.received_values[self.task]:
                        self.expected_values[self.task][node.node_identifier] = node.node_identifier #can be any value    
           
                # Send data to each node         
                for receiving_node in self.other_nodes:
                    self.env.process(self.send_data(receiving_node, self.node_identifier, self.task, 'Sent data', self.last_rb_checkpoint, self.other_nodes_rb_checkpoints[receiving_node.node_identifier]))
                self.task += 1 #starting next task 
            except simpy.Interrupt:
                yield self.env.timeout(self.rollback_cost)
                if self.if_rollback_in_rollback:
                    yield self.env.timeout(self.rollback_cost)
                    self.if_rollback_in_rollback = False 
                self.remove_values(self.task)
                self.merge_dictionaries()
                self.queue.clear() 
                self.is_rollback = False 
                print(f"Node {self.node_identifier} has finished its rollback to task {self.task} (time {self.env.now}) \n")
                sys.stdout.flush()
                
    # Process which sends data to all nodes (excluding itself)
    def send_data(self, receiving_node, sending_node, sending_node_task, message, sending_node_lastrb, receiving_node_lastrb):
        if message == 'Sent data':
            print(f"Node {sending_node} is sending task {sending_node_task} data to Node {receiving_node.node_identifier} with message: {message} (time {self.env.now})")
        else:
            print(f"Node {sending_node} is sending a message to Node {receiving_node.node_identifier} with message: {message} for task {sending_node_task} (time: {self.env.now}) \n")
        sys.stdout.flush()   
        yield self.env.timeout(self.network_delay)
        self.num_data_shared += 1
        receiving_node.receive_data(sending_node, sending_node_task, message, sending_node_lastrb, receiving_node_lastrb)
        
    # Process which recieves data sent by other nodes
    def receive_data(self, sending_node, sending_node_task, message, sending_node_lastrb, receiving_node_lastrb):
        
        # Updates the last rollback checkpoint for the sending node if it has been changed 
        if (self.other_nodes_rb_checkpoints[sending_node] != sending_node_lastrb):
            self.other_nodes_rb_checkpoints[sending_node] = sending_node_lastrb
        
        if message == 'wrong expected value':
            print(f"Node {self.node_identifier} has received an incorrect value from Node {sending_node} for task {sending_node_task} (time: {self.env.now}) \n")
            sys.stdout.flush()
            self.rollback(sending_node_task)
        else: 
            print(f"Node {self.node_identifier} has received a value for task {sending_node_task} from Node {sending_node} (time: {self.env.now}) \n")
            sys.stdout.flush()
            # Initialising dict for received and expected values
            if sending_node_task not in self.received_values:
                    self.received_values[sending_node_task] = {}  
            if sending_node_task not in self.expected_values:
                    self.expected_values[sending_node_task] = {}              
            if (self.last_rb_checkpoint is not None and receiving_node_lastrb is not None): 
                if (sending_node_task >= self.last_rb_checkpoint and receiving_node_lastrb != self.last_rb_checkpoint): 
                    print(f"Node {self.node_identifier}'s last checkpoint value is {self.last_rb_checkpoint} but Node {sending_node}'s last checkpoint value for Node {self.node_identifier} is {receiving_node_lastrb} (time: {self.env.now}) \n")  
                    sys.stdout.flush()
                    return                 
            if not(self.is_rollback): 
                # Placing recieved value in the recieved_value dictionary        
                if sending_node not in self.received_values[sending_node_task]:
                    self.received_values[sending_node_task][sending_node] = sending_node 
                # Expected value and rollback check     
                if self.check_dictionaries(sending_node_task, sending_node, [self.received_values, self.expected_values]):
                    self.num_expected_values_checked += 1
                    # Removed expected value as we now received the value 
                    del self.expected_values[sending_node_task][sending_node] 
                    if not(self.check_expected_value()):
                        print(f"Node {self.node_identifier}'s expected value for Node {sending_node} at task {sending_node_task} was wrong (time: {self.env.now}) \n")
                        sys.stdout.flush()
                        # Broadcast to nodes that a task calculation used an incorrect value 
                        for receiving_node in self.other_nodes: 
                             self.env.process(self.send_data(receiving_node, self.node_identifier, sending_node_task, 'wrong expected value', sending_node_task, self.other_nodes_rb_checkpoints[receiving_node.node_identifier]))
                        self.last_rb_checkpoint = sending_node_task
                        # Rollback to that task 
                        self.rollback(sending_node_task)
            else: 
                # Node is in a rollback
                if sending_node_task not in self.queue:
                        self.queue[sending_node_task] = {} 
                self.queue[sending_node_task][sending_node] = sending_node #can be any value
        
    
    #Process which performs a rollback for a node 
    def rollback(self, rollback_task):
        # Node hasnt reached task yet 
        if self.task == rollback_task:
            print(f"Node {self.node_identifier} will not rollback to {rollback_task} as it is already at task {rollback_task} (time: {self.env.now}) \n")
            sys.stdout.flush()
            return
        elif self.task < rollback_task: 
            # If node has not reached the rollback task, we only remove the received values from that point (and all expected values from and including that task)
            print(f"Node {self.node_identifier} will not rollback to task {rollback_task} as it is only on task {self.task} (time: {self.env.now}) \n")   
            sys.stdout.flush()
            self.remove_values(rollback_task) 
        else: 
            print(f"Node {self.node_identifier} has rolled back {self.num_rollbacks} times")
            print(f"Node {self.node_identifier} is rolling back to task {rollback_task} from task {self.task} (time: {self.env.now}) \n")
            sys.stdout.flush()
            difference = self.task - rollback_task
            self.total_tasks_wasted += difference
            self.num_rollbacks += 1
            if not(self.is_rollback):
               self.is_rollback = True 
               self.task = rollback_task 
               self.running_calculate.interrupt()
            else: #its in a rollback 
               self.if_rollback_in_rollback = True
               self.task = rollback_task 
               print(f"Node {self.node_identifier} is already in a rollback to {self.task} but will now go further back to {rollback_task} (time: {self.env.now}) \n")
               sys.stdout.flush()
    
    # Function which removes expected values and recieved values from task x onwards 
    def remove_values(self, checkpint_to_delete_from):
        max_task_EV = max(self.expected_values.keys(), default=checkpint_to_delete_from) 
        max_task_RV = max(self.received_values.keys(), default=checkpint_to_delete_from)
        for i in range(checkpint_to_delete_from, max_task_EV + 1):
            if i in self.expected_values:
                self.expected_values[i].clear() 
        for i in range(checkpint_to_delete_from + 1, max_task_RV + 1): 
            if i in self.received_values: 
                self.received_values[i].clear() 
    
    # Returns the task dictionary 
    def return_received_value_dict(self, task):
        if task in self.received_values:
            return self.received_values[task]
        return False 
    
    # Rreturns the number of rollbacks completed by the node
    def return_num_rollbacks(self):
        return self.num_rollbacks 
    
    # Returns the number of times that the node sent data 
    def return_data_shared(self):
        return self.num_data_shared
    
    # Returns the number of expected values checked 
    def return_expected_values_checked(self):
        return self.num_expected_values_checked
    
    # Returns the task which each node is on 
    def return_task(self):
        return self.task
    
    # Returns the total cycles wasted 
    def return_wasted_cycles(self):
         return self.total_tasks_wasted * self.task_length
        
# Class: Distributed System
class DistributedSystem(object):
    
    # Initialise parameters for distributed system
    def __init__(self, env, sim_time, num_nodes):
       self.env = env 
       self.sim_time = sim_time
       self.num_nodes = num_nodes     
       self.start_nodes = simpy.Event(env)  
       self.total_task_length_per_checkpoint = 0 #total task length 
  
    # Creating the nodes for the simulation    
    def create_nodes(self, task_lengths, cpu_speeds, predicted_error_rate, network_delay, rollback_cost):
        self.nodes_created = []
        for node_identifier in range(self.num_nodes): 
            new_node = Nodes(node_identifier, task_lengths[node_identifier], cpu_speeds[node_identifier], 
                             predicted_error_rate, network_delay, rollback_cost, self.num_nodes, self.env, self.start_nodes)
            self.nodes_created.append(new_node)
            
        for task in task_lengths:
            self.total_task_length_per_checkpoint += task 
     
    # Function which sends n-1 nodes to each node        
    def send_nodes(self):
        for cur_node in self.nodes_created:
            other_nodes = []
            for node in self.nodes_created:
                if node != cur_node:
                    other_nodes.append(node)
            cur_node.store_other_nodes(other_nodes)
    
    # Returns the total number of rollbacks
    def return_total_num_rollbacks(self):
        total_num_rollbacks = 0 
        for node in self.nodes_created:
            total_num_rollbacks += node.return_num_rollbacks() 
        return total_num_rollbacks     
    
    # Returns the total number of times that data is shared 
    def return_total_data_shared(self):
        total_num_data_shared = 0 
        for node in self.nodes_created:
            total_num_data_shared += node.return_data_shared() 
        return total_num_data_shared
    
    # Returns the number of expected values checked
    def return_num_expected_values_checked(self):
        total_num_expected_values = 0 
        for node in self.nodes_created:
            total_num_expected_values += node.return_expected_values_checked() 
        return total_num_expected_values
    
    # Function which determines the throughput and returns it
    def calculate_and_return_throughput(self):
        # If all n nodes have 0 expected values and n-1 recieved values for a task, then we increase throughput by 1
        # If all n nodes have n-1 recieved values for a task, then we increase throughput by 1 as we know EV dictionary will be empty
        checkpoints = 0 
        cur_task = 0 
        while True:
            for node in self.nodes_created:
                rv_inner_dict = node.return_received_value_dict(cur_task)
                if not(rv_inner_dict):
                    return checkpoints * self.total_task_length_per_checkpoint
                if (len(rv_inner_dict) == self.num_nodes-1):
                    # Nodes must have completed task with the received values
                    if (node.return_task == cur_task): 
                        return checkpoints * self.total_task_length_per_checkpoint
                    else:
                        continue 
                else:
                    return checkpoints * self.total_task_length_per_checkpoint    
            checkpoints += 1
            cur_task += 1   
    
    # Function which returns back the total number of tasks completed     
    def return_tasks_completed(self):
        checkpoints = 0 
        cur_task = 0 
        while True:
            for node in self.nodes_created:
                rv_inner_dict = node.return_received_value_dict(cur_task)
                if not(rv_inner_dict):
                    return checkpoints 
                if (len(rv_inner_dict) == self.num_nodes-1):
                    if (node.return_task == cur_task):
                        return checkpoints 
                    else:
                        continue 
                else:
                    return checkpoints    
            checkpoints += 1
            cur_task += 1   
      
    # Function which returns the total cycles wasted due to rollbacks. 
    # This is the sum of the (individual node task length * wasted checkpoints)    
    def total_cycles_wasted(self):
        wasted_cycles = 0 
        for node in self.nodes_created:
            wasted_cycles += node.return_wasted_cycles()
        return wasted_cycles
    
    #Function which initialises the rollback lists for each node   
    def initialise_rollback_lists(self):
        for node in self.nodes_created:
            node.create_rollback_lists()
            
    # Function which runs the simulation 
    def run_sim(self):
        self.start_nodes.succeed()
        self.env.run(until=self.sim_time) 


#------------------------------------------------------------------------------------------------------------------------------------          

def run_simulation(inputs):

    num_nodes = inputs['number_of_nodes']
    
    task_lengths = inputs['node_task_lengths']
    
    cpu_speeds = inputs['node_cpu_speeds']
    
    predicted_error_rate = inputs['predicted_error_rate']
    
    network_delay = inputs['network_delay']
    
    rollback_cost = inputs['rollback_cost']
    
    env = simpy.Environment()
    sim_time = inputs['sim_time_seconds']  #in seconds
    DS = DistributedSystem(env, sim_time, num_nodes)
    DS.create_nodes(task_lengths, cpu_speeds, predicted_error_rate, network_delay, rollback_cost)
    DS.send_nodes()
    DS.initialise_rollback_lists() 
    DS.run_sim()
    
    # Final outputs
    print("\n")
    print(f"The total number of rollbacks was: {DS.return_total_num_rollbacks()}")
    print(f"The total time of the rollbacks was: {DS.return_total_num_rollbacks() * rollback_cost} seconds")
    print(f"The total number of times that data was shared was: {DS.return_total_data_shared()}")
    print(f"The rate at which data was shared by a node was every {(DS.return_total_data_shared() / sim_time):02.2f} seconds")
    print(f"The total cycle throughput was: {DS.calculate_and_return_throughput()} cycles")
    print(f"The total number of tasks completed was: {DS.return_tasks_completed()}")
    print(f"The total number of wasted cycles was: {DS.total_cycles_wasted()} cycles")
    print(f"The total number of expected values checked was: {DS.return_num_expected_values_checked()}")
    
with open('system_conditions.json', 'r') as opened_file:
    # Inputs which represent the system conditions
    inputs = json.load(opened_file) 
    run_simulation(inputs)


