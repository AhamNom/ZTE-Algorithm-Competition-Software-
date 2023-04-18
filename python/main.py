import heapq
import csv
import os

def read_data(file_path):
    data = []
    with open(file_path, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)  # skip header
        for row in reader:
            data.append(list(map(int, row)))
    return data

def scheduler(flow_data, port_data):
    flow_data.sort(key=lambda x: x[2])  # sort by enter_time
    port_heap = [(port[0], port[1], 0) for port in port_data]
    heapq.heapify(port_heap)
    waiting_queue = []
    results = []

    for flow in flow_data:
        found_port = False
        exhausted_ports = []
        while port_heap:
            port_id, port_bandwidth, next_available_time = heapq.heappop(port_heap)
            if flow[2] >= next_available_time and flow[1] <= port_bandwidth:
                results.append((flow[0], port_id, max(flow[2], next_available_time)))
                heapq.heappush(port_heap, (port_id, port_bandwidth - flow[1], next_available_time + flow[3]))
                found_port = True
                break
            exhausted_ports.append((port_id, port_bandwidth, next_available_time))

        # Return the exhausted ports back to the heap
        for port in exhausted_ports:
            heapq.heappush(port_heap, port)

        if not found_port:
            waiting_queue.append(flow)

    while waiting_queue:
        flow = waiting_queue.pop(0)
        _, port_bandwidth, next_available_time = heapq.heappop(port_heap)
        results.append((flow[0], port_id, max(flow[2], next_available_time)))
        heapq.heappush(port_heap, (port_id, port_bandwidth - flow[1], next_available_time + flow[3]))

    return results

def save_results(results, output_path):
    with open(output_path, 'w') as f:
        writer = csv.writer(f)
        # writer.writerow(['流id', '端口id', '开始发送时间'])
        for result in results:
            writer.writerow(result)

def main(seqnum):
    flow_data = read_data('../data/' + str(seqnum) + '/flow.txt')
    port_data = read_data('../data/' + str(seqnum) + '/port.txt')
    results = scheduler(flow_data, port_data)
    save_results(results, '../data/' + str(seqnum) + '/result.txt')

if __name__ == "__main__":
    testnum = 0
    while os.path.isdir('../data/' + str(testnum)):
        main(testnum)
        testnum += 1
