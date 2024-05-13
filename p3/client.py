import csv
import sys #sys.argv
import threading #threading/lock
import grpc #create gtpc stubs
import mathdb_pb2
import mathdb_pb2_grpc


# Global lists for storing hits and total requests per thread
hits = []
tot_reqs = []


def process_csv(filename, stub, thread_index):
  local_hits = 0
  local_total = 0
  with open(filename, 'r') as file:
      csv_reader = csv.reader(file)
      for row in csv_reader:
          command = row[0]
          key_a= row[1]
          key_b =  None if len(row) < 3 else row[2]
          try:
              if command.upper() == "SET":
                  stub.Set(mathdb_pb2.SetRequest(key=key_a, value=float(key_b)))
              elif command.upper() == "GET":
                  response = stub.Get(mathdb_pb2.GetRequest(key=key_a))
              elif command.upper() in ["ADD", "SUB", "MULT", "DIV"]:
                  # Perform the operation and check for cache hit
                  if command.upper() == "ADD":
                      response = stub.Add(mathdb_pb2.BinaryOpRequest(key_a=key_a, key_b=key_b))
                  elif command.upper() == "SUB":
                      response = stub.Sub(mathdb_pb2.BinaryOpRequest(key_a=key_a, key_b=key_b))
                  elif command.upper() == "MULT":
                      response = stub.Mult(mathdb_pb2.BinaryOpRequest(key_a=key_a, key_b=key_b))
                  elif command.upper() == "DIV":
                      response = stub.Div(mathdb_pb2.BinaryOpRequest(key_a=key_a, key_b=key_b))
                  local_hits += int(response.cache_hit)
                  local_total += 1


          except grpc.RpcError as e:
              print(f"error: {e}")


  hits[thread_index] = local_hits
  tot_reqs[thread_index] = local_total


def main():
  if len(sys.argv) < 3:
      sys.exit(1)


  port = sys.argv[1]
  csv_files = sys.argv[2:]


  channel = grpc.insecure_channel(f'localhost:{port}')
  stub = mathdb_pb2_grpc.MathDbStub(channel)


  global hits, tot_reqs
  hits = [0] * len(csv_files)
  tot_reqs= [0] * len(csv_files)


  my_threads = []


  for i, filename in enumerate(csv_files): #to get a counter in a loop
      thread = threading.Thread(target=process_csv, args=(filename, stub, i))
      thread.start()
      my_threads.append(thread)


  # wait
  for thread in my_threads:
      thread.join()


  sum_hits = sum(hits)
  sum_reqs = sum(tot_reqs)
  hit_rate = sum_hits / sum_reqs if sum_reqs else 0
  print(hit_rate)


if __name__ == '__main__':
  main()
