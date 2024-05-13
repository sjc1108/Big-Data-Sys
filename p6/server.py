import cassandra
from cassandra import ConsistencyLevel
import grpc
import station_pb2
import station_pb2_grpc
from concurrent import futures
from cassandra.cluster import Cluster



class Records:
    def __init__(self, tmin, tmax):
        self.tmin = tmin
        self.tmax = tmax


class StationServicer(station_pb2_grpc.StationServicer):
    
    def __init__(self):
        cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.cass = cluster.connect()
        cluster.register_user_type("weather", "station_record", Records)

    def RecordTemps(self, request, context): #method for RecordTemps requests
        try:
             #insert statement for cass
            insert_statement = self.cass.prepare("""INSERT INTO weather.stations (id, date, record) VALUES (?, ?, ?)""")
            insert_statement.consistency_level = ConsistencyLevel.ONE
            self.cass.execute(insert_statement, (request.station, request.date, Records(request.tmin, request.tmax)))
            
            return station_pb2.RecordTempsReply(error = "")
            
        except cassandra.Unavailable as e: # for specific cass except
            return station_pb2.RecordTempsReply(error = "need {need} replicas, but only have {have}".format(need = e.required_replicas, have = e.alive_replicas))  
            
        except cassandra.cluster.NoHostAvailable as e: # if no hosts avail
            
            for i in e.errors.values():
                
                if isinstance(i, cassandra.Unavailable):
                    
                    return station_pb2.RecordTempsReply(error = "need {need} replicas, but only have {have}".format(need = e.required_replicas, have = e.alive_replicas)) 
                    
                else:
                    return station_pb2.RecordTempsReply(error = "is other Cassandra error") 
                    
        except Exception as e:
            return station_pb2.RecordTempsReply(error = str(e))

    # gRPC to retrieve max temp
    def StationMax(self, request, context):
        try:
            
            max_statement = self.cass.prepare("SELECT MAX(record.tmax) FROM weather.stations WHERE id=?")
            max_statement.consistency_level = ConsistencyLevel.THREE
            result = self.cass.execute(max_statement, (request.station,)) # execute query and get max temp
            
            return station_pb2.StationMaxReply(tmax = result.one()[0], error = "")
            
        except cassandra.Unavailable as e: #for specific cass except
            return station_pb2.StationMaxReply(error= "need {need} replicas, but only have {have}".format(need= e.required_replicas,have = e.alive_replicas))
            
        except cassandra.cluster.NoHostAvailable as e:
            
            for i in e.errors.values():
                
                if isinstance(i, cassandra.Unavailable):
                    
                    return station_pb2.RecordTempsReply(error = "need {need} replicas, but only have {have}".format(need = e.required_replicas, have=e.alive_replicas)) 
                    
                else:
                    return station_pb2.RecordTempsReply(error = "is other Cassandra error") 
                    
        except Exception as e:
            return station_pb2.StationMaxReply(error = str(e))

def main():
    print("starting server")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1), options=(('grpc.so_reuseport', 0),))
    station_pb2_grpc.add_StationServicer_to_server(StationServicer(), server)
    server.add_insecure_port("[::]:5440", )
    server.start()
    server.wait_for_termination()
    
if __name__ == "__main__":
	main()