import argparse
from urllib.request import urlopen
import csv
import re
import time


class Queue():
    
    def __init__(self):
        self.items = []

    def items(self):
        return self.items

    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)


class Server():
    
    def __init__(self):
        """intialize an instance"""

        self.current_request = None
        self.time_remaining = 0 #internal timer

    def tick(self):
        """Decrements the internal timer sets the server to idle when task completed"""

        if self.current_request != None:
            self.time_remaining = self.time_remaining - 1 
            if self.time_remaining <= 0:
                self.current_request = None

    def busy(self):
        if self.current_request != None:
            return True
        else:
            return False

    def start_next(self, new_request):
        self.current_request = new_request
        self.time_remaining = new_request.get_processing_time() 


class Request():
    """represents a single server request"""

    def __init__(self, time, processing_time):
        self.timestamp = time # used for computing wait time —- time request was created/place in queue
        self.processing_time = processing_time # row[2] from csv_reader

    def get_stamp(self):
        return self.timestamp

    def get_processing_time(self):
        return self.processing_time

    def wait_time(self, current_time):
        if current_time - self.timestamp <= 0:
            #print(self.processing_time)
            return self.processing_time
        else:
            return current_time - self.timestamp #amt of time spent in queue before request was processed
            #print(current_time - self.timestamp)


def simulateOneServer(file): 

    web_server = Server()
    request_queue = Queue()
    waiting_times = [] 

 
    csv_file = urlopen(file)
    csv_list = [i.decode("utf-8") for i in csv_file] #decode csv and store each row as a string in a big list
    csv_reader = csv.reader(csv_list, delimiter=',') #take each row (single string) and and break each string into separate elements of a smaller list
    requests = [[int(row[0]), row[1], int(row[2])] for row in csv_reader] #converts row[0] and row[2] from csv to ints
    
   

    for current_second in range(len(requests)):
        
        request = Request(requests[current_second][0], requests[current_second][2])
        request_queue.enqueue(request)
        
        if (not web_server.busy()) and (not request_queue.is_empty()):
            next_request = request_queue.dequeue()
            waiting_times.append(next_request.wait_time(current_second)) 
            web_server.start_next(next_request)
        
        web_server.tick()
    
    average_wait = sum(waiting_times) / len(waiting_times)
    print("Average wait %1.1f secs. %i requests remaining." % (average_wait, request_queue.size()))


def simulateManyServers(file, servers):

    web_server = Server()
    queue_list = Queue()
    waiting_times = [] 
    server_count = servers
    counter = 0

    
    csv_file = urlopen(file)
    csv_list = [i.decode("utf-8") for i in csv_file] #decode csv and store each row as a string in a big list
    csv_reader = csv.reader(csv_list, delimiter=',') #take each row (single string) and and break each string into separate elements of a smaller list
    requests = [[int(row[0]), row[1], int(row[2])] for row in csv_reader] #converts row[0] and row[2] from csv to ints
    
    queue_list = [Queue for i in range(server_count)]

    queue_wait_avgs = []

    for r in range(len(requests)):



        request = Request(requests[r][0], requests[r][2])
        queue_list[counter].enqueue(request)
        
        if counter < server_count - 1:
            counter += 1                                        
        else:
            counter = 0 

    for assignment in range(server_count):
        current_request = queue_list[assignment].dequeue()

        for current_second in range(queue_list[assignment].size()):
            if(not web_server.busy()) and (not queue_list[assignment].is_empty()):
                next_request = queue_list[assignment].dequeue()
                waiting_times.append(next_request.wait_time(current_request.get_stamp + current_request.get_processing_time()))
                web_server.start_next(next_request)
                current_request = next_request
            web_server.tick()
    
    
        queue_wait_avgs.append(sum(waiting_times) / len(waiting_times))

    total = 0

    for t in range(len(queue_wait_avgs)):
        total += queue_wait_avgs[t]
    total_avg_wait = total / server_count

    print("Average wait %1.1f secs when balancing load across %i servers." % (total_avg_wait, server_count))
        


def main(file, servers):

    if servers == 1:
        simulateOneServer(file)
    
    elif 1 < servers <= 3:
        simulateManyServers(file, servers)


    else:
        print("Choose between 1 and 3 servers")



if __name__ == "__main__":
    """Main entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="URL to the datafile", type=str, required=True)
    parser.add_argument("--servers", help="Number of Servers", type=int, required=True)
    args = parser.parse_args()

    file = args.file
    servers = args.servers
    main(file, servers)
