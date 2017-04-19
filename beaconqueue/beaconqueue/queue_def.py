#!/usr/bin/python
#-*-coding:utf-8-*-
import pymongo
from pymongo import MongoClient
from pymongo import errors as pymongo_errors

try:
    from .settings import MongoAddr, MongoClientConfig
except:
    from settings import MongoAddr, MongoClientConfig

QUEUE_DB_NAME = "beacon_qs"

class QUEUE_DEFS:
    WAITING_Q = 0
    RUNNING_Q = 1
    DONE_Q    = 2

class QueueBase(object):
    def __init__(self, mongo_addr, queue_collection_name):
        self._mongo_host      = mongo_addr.host
        self._mongo_port      = mongo_addr.port
        self._collection_name = queue_collection_name
        self._mongo_info = {
            "host"                : self._mongo_host,
            "port"                : self._mongo_port,
            "collection"          : self._collection_name,
            "socketTimeoutMS"     : MongoClientConfig.socketTimeoutMS,
            "connectionTimeoutMS" : MongoClientConfig.connectTimeoutMS
        }

    @property
    def queue_collection(self):
        client     = MongoClient(
            self._mongo_host,
            self._mongo_port,
            socketTimeoutMS  = MongoClientConfig.socketTimeoutMS,
            connectTimeoutMS = MongoClientConfig.connectTimeoutMS
        )
        db         = client[QUEUE_DB_NAME]
        collection = db[self._collection_name]
        return collection

    def __do(self, action_name, action):
        err = {"action": action_name}
        try:
            result = action()
        except pymongo_errors.ConnectionFailure:
            err["error"] = "ConnectionFailure"
            err.update(self._mongo_info)
        except pymongo_errors.ExecutionTimeout:
            err["error"] = "ExecutionTimeout"
        except pymongo_errors.NetworkTimeout:
            err["error"] = "NetworkTimeout"
        except Exception as e:
            err["error"] = type(e)
        if err.get("error"):
            err.update(self._mongo_info)
            return err
        else:
            return {"result": result, "status": "OK"}

    def put(self, data, check_unique=False,
            unique_fields_except=["_id", "priority", "job_id", "create_time"]):
        unique_condition = {}
        for k in data:
            if k in unique_fields_except:
                continue
            unique_condition[k] = data[k]
        def exists_data():
            r = self.queue_collection.find_one(unique_condition)
            if r:
                return True
            else:
                return False

        if check_unique and exists_data():
            r = self.__do(
                "put", lambda: self.queue_collection.update_one(
                    unique_condition,
                    {"$set": {"priority": data.get("priority") or 100}}
                ))
        else:
            r = self.__do(
                "put", lambda : self.queue_collection.insert_one(data))
        if r.get("error"):
            return r
        else:
            r["result"] = True
            return r

    def fetch_one_and_delete(self, condition,
                             sort=[("priority", "DESCENDING"),
                                   ("create_time", "ASCENDING")]):
        s = []
        for k, direction in sort:
            s.append((k, pymongo.ASCENDING if direction=="ASCENDING" else pymongo.DESCENDING))
        return self.__do(
            "fetch_one_and_delete",
            lambda : self.queue_collection.find_one_and_delete(
                condition, {"_id": False}, sort=s))

    def count(self, condition):
        return self.__do(
            "count",
            lambda: self.queue_collection.find(condition).count()
        )

    def get(self, condition, return_fields):
        """
        :param condition: <dict> mongo查询条件
        :param return_fields: <dict> key为字段明, 对应值为True则返回, 为False则不返回
        """
        if return_fields.get("_id") is None:
            return_fields["_id"] = False
        return self.__do(
            "get",
            lambda: list(self.queue_collection.find(condition, return_fields))
        )


class WaitingQueue(QueueBase):
    def __init__(self, mongo_addr):
        super(WaitingQueue, self).__init__(mongo_addr, "waiting_q")

class RunningQueue(QueueBase):
    def __init__(self, mongo_addr):
        super(RunningQueue, self).__init__(mongo_addr, "running_q")

class DoneQueue(QueueBase):
    def __init__(self, mongo_addr):
        super(DoneQueue, self).__init__(mongo_addr, "done_q")

if __name__ == '__main__':
    q = WaitingQueue(MongoAddr)
    q.put({"a": 1})
    print(q.fetch_one_and_delete({"a": 1}))
