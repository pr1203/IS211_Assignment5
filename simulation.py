import csv
import argparse
from urllib import request


# Request class defines each request
class Request:
    def __init__(self, request_time, resource_name, processing_time):
        # Which second it was generated for
        self.request_time = request_time
        # File name
        self.file_name = resource_name
        # Time needed to finish
        self.processing_time = processing_time

        # Time took requests for waiting
        self.wait_time = 0
        # Time took to process
        self.progress_time = 0

    def update(self):
        self.wait_time += 1


class Server:
    def __init__(self):
        # List container for queue request
        self.request_queue = []

    def put_queue(self, request: Request):
        # Appending queue request to list container
        self.request_queue.append(request)

    def update_queue(self) -> Request:
        queue_list = len(self.request_queue)
        current_request = self.request_queue[0]

        if (self.request_queue):
            for i in range(0, queue_list):
                if (i == 0):
                    self.request_queue[i].progress_time += 1

        elif (current_request.progress_time == current_request.processing_time):
            return_value = self.request_queue.pop(0)

        elif (not self.request_queue):
            return_value = None

        return return_value


def simulateOneServer(requests):
    req_processed = 0
    timer = 0
    total_wait_time = 0
    req_num = len(requests)

    if (req_num == 1):
        return 0

    server = Server()

    while (req_processed != req_num):
        for req in requests:
            if (req.reqeust_time == timer):
                server.put_queue(req)

        req_done = server.update_queue()

        if (req_done != None):
            total_wait_time += req_done.wait_time
            req_processed += 1

        timer += 1
    return float(total_wait_time / req_num)


def main(fileName):
    req = []
    try:
        with open(fileName) as f:
            req_list = csv.reader(f)
            for line in req_list:
                request_time = int(line[0].strip())
                file_name = line[1].strip()
                processing_time = int(line[2].strip())
                req.append(Request(request_time, file_name, processing_time))
    except:
        print(f"Error occurred. Either {fileName} does not exist or other error.")

    if (len(req) < 1):
        print("Request is less than 1.")

    avg_wait_time = simulateOneServer(req)

    print(f"File {fileName} has {len(req)} requests and the average wait time is {avg_wait_time:2f}.")

    if __name__ == '__main__':
        parser = argparse.ArgumentParser(description="Printer Program")
        parser.add_argument("--file", help="Provide path of the file")
        args = parser.parse_args()
        main(args)
