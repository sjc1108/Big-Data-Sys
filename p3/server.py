from collections import OrderedDict
import grpc #create gtpc stubs
from concurrent import futures
import mathdb_pb2
import mathdb_pb2_grpc
import traceback
import threading

class MathCache:
    def __init__(self): #has two dict cache of key-val
        self.cache = {}
        self.lru_cache = OrderedDict()

        self.cache_limit = 10
        self.lock = threading.Lock()

    def Set(self, key, value): #to update self.cache
        with self.lock:
            self.cache[key] = value
            self.lru_cache.clear()

    def Get(self, key): #It gets key fr self.cache
        with self.lock:
            if key not in self.cache: 
                raise KeyError(f'Key {key} not found in cache.')   
            
            return self.cache[key] 


    def Add(self, key_a, key_b): # math on the values associated with keys
        return self._operation("add", key_a, key_b)

    def Sub(self, key_a, key_b): #m
        return self._operation("sub", key_a, key_b)

    def Mult(self, key_a, key_b): #m
        return self._operation("mult", key_a, key_b)

    def Div(self, key_a, key_b): #m
        return self._operation("div", key_a, key_b)

    def _operation(self, op, key_a, key_b): #performs (add, sub, mult, div) if both keys are in cache 
        with self.lock: 
            if key_a not in self.cache or key_b not in self.cache:
                raise KeyError('One or both keys not found in cache.')
            
            cache_key = (op, key_a, key_b)

            if cache_key in self.lru_cache:
                self.lru_cache.move_to_end(cache_key)
                return self.lru_cache[cache_key], True

            if op == "add":
                result = self.cache[key_a] + self.cache[key_b]
            elif op == "sub":
                result = self.cache[key_a] - self.cache[key_b]
            elif op == "mult":
                result = self.cache[key_a] * self.cache[key_b]
            elif op == "div":
                result = self.cache[key_a] /self.cache[key_b]

            self.lru_cache[cache_key] = result

            if len(self.lru_cache) > self.cache_limit:
                self.lru_cache.popitem(last = False)

            return result, False


class MathDb(mathdb_pb2_grpc.MathDbServicer): #method for calls corresponding method on mathcache & returns a gRPC
    def __init__(self):
        self.math_cache = MathCache()

    def Set(self, request, context): 
        try:
            self.math_cache.Set(request.key, request.value)
            return mathdb_pb2.SetResponse(error = "")
        
        except Exception as e:
            return mathdb_pb2.SetResponse(error = str(traceback.format_exc()))

    def Get(self, request, context):
        try:
            value = self.math_cache.Get(request.key)
            return mathdb_pb2.GetResponse(value =value, error = "")
        
        except Exception as e:
            return mathdb_pb2.GetResponse(value = 0.0, error = str(traceback.format_exc()))

    def Add(self, request, context):
        try:
            result, cache_hit = self.math_cache.Add(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value = result, cache_hit = cache_hit, error = "")
        
        except Exception as e:
            return mathdb_pb2.BinaryOpResponse(value = 0.0, cache_hit = False, error = str(traceback.format_exc()))

    def Sub(self, request, context):
        try:
            result, cache_hit = self.math_cache.Sub(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value = result, cache_hit =cache_hit, error = "")
        
        except Exception as e:
            return mathdb_pb2.BinaryOpResponse(value = 0.0, cache_hit = False, error = str(traceback.format_exc()))

    def Mult(self, request, context):
        try:
            result, cache_hit = self.math_cache.Mult(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value = result, cache_hit = cache_hit, error = "")
        
        except Exception as e:
            return mathdb_pb2.BinaryOpResponse(value= 0.0, cache_hit = False, error = str(traceback.format_exc()))

    def Div(self, request, context):
        try:
            result, cache_hit = self.math_cache.Div(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value = result, cache_hit = cache_hit, error = "")
        
        except Exception as e:
            return mathdb_pb2.BinaryOpResponse(value = 0.0, cache_hit= False, error = str(traceback.format_exc()))

if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers = 4), options = (("grpc.so_reuseport", 0),))
    mathdb_pb2_grpc.add_MathDbServicer_to_server(MathDb(), server)
    server.add_insecure_port('[::]:5440')
    server.start()
    server.wait_for_termination()
