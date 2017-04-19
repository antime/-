#!/usr/bin/python
#-*-coding:utf-8-*-
import json
from uuid import uuid4
from datetime import datetime
from copy import deepcopy

from rpyc import Service
from rpyc.utils.server import ThreadedServer

from settings import MongoAddr, RpcConfig
from queue_def import WaitingQueue, RunningQueue, DoneQueue, QUEUE_DEFS

class QueueAction(object):
    def __init__(self, mongo_addr):
        self._waiting_q = WaitingQueue(mongo_addr)
        self._running_q = RunningQueue(mongo_addr)
        self._done_q    = DoneQueue(mongo_addr)

    def register(self, job_type, job_kwargs, job_identifiers, priority=100):
        """
        :param job_type: <str> 任务类型名
        :param job_kwargs: <dict> 任务参数表
        :param job_identifiers: <dict> 任务标识符, 一般有 target_id, scan_id
        """
        job_id = uuid4().hex
        job_data = {
            "type"   : job_type,
            "kwargs" : job_kwargs,
            "job_id" : job_id,
            "priority" : priority,
            "create_time" : datetime.now()
        }
        job_data.update(job_identifiers)
        return self._waiting_q.put(job_data, check_unique=True)

    def fetch(self, job_type, by_who, num=1):
        """
        :param job_type: <str> 任务类型名
        :param by_who: <str> 谁(哪个节点)抓走了该任务
        :param num: <int> 获取任务的个数

        如果足够或未发生异常, 返回 num 个任务.
        """
        jobs = []
        for _ in range(num):
            job = self._waiting_q.fetch_one_and_delete({"type": job_type})
            result = job.get("result")
            status = job.get("status")
            if result and status == "OK":
                jobs.append(result)
                r = deepcopy(result)
                r["node"] = by_who
                r["start_time"] = datetime.now()
                self._running_q.put(r)
        return jobs

    def finishing(self, job_id, result_addr, job_identifer_names):
        """
        :param job_id: <str> 任务id
        :param result_addr: <str> 该任务查询地址(使用json数据)
        :param job_identifier_names: <list> 任务中作为标识符的字段
        """
        r = self._running_q.fetch_one_and_delete({"job_id": job_id})
        status = r.get("status")
        job = r.get("result")
        if not job or status != "OK":
            return r
        job_idents = {}
        for ident_name in job_identifer_names:
            ident_value = job.get(ident_name)
            if ident_value:
                job_idents[ident_name] = ident_value
        job["finish_time"] = datetime.now()
        job["result_addr"] = result_addr
        return self._done_q.put(job)

    def inspect(self, which_queue, condition,
                return_count=True, return_fields={}):
        """
        :param which_queue: <enum QUEUE_DEFS> 查看哪张表的状态
        :param condition: <dict> 查询条件
        :param return_count: <bool> 为True, 返回符合查询条件的条目数量
        :param return_fields: <dict> 若return_count为False, 返回该参数中值为True的字段的值
        """
        qs = {
            QUEUE_DEFS.WAITING_Q: self._waiting_q,
            QUEUE_DEFS.RUNNING_Q: self._running_q,
            QUEUE_DEFS.DONE_Q   : self._done_q
        }
        q = qs.get(which_queue)
        if not q:
            return {"result": "not found"}
        if return_count:
            return q.count(condition)
        else:
            return q.get(condition, return_fields)

class QueueServer(Service):
    def __init__(self, *args, **kwargs):
        self.queue_action = QueueAction(MongoAddr)

    def exposed_register(self, job_type, job_kwargs, job_identifiers, priority=100):
        """
        见QueueAction.register
        """
        return self.queue_action.register(
            job_type, job_kwargs, job_identifiers, priority)

    def exposed_fetch(self, job_type, by_who, num=1):
        """
        见QueueAction.fetch
        """
        return self.queue_action.fetch(job_type, by_who, num)

    def exposed_finishing(self, job_id, result_addr, job_identifer_names):
        """
        见QueueAction.finishing
        """
        return self.queue_action.finishing(
            job_id, result_addr, job_identifer_names)

    def exposed_inspect(self, which_queue, condition,
                        return_count=True, return_fields={}):
        """
        见QueueAction.inspect
        """
        return self.queue_action.inspect(
            which_queue, condition, return_count, return_fields
        )

server = ThreadedServer(
    QueueServer, port=RpcConfig.port,
    protocol_config = {"allow_public_attrs" : True}
)
print("""
**************************************
***   RPC Target Server Running!   ***
**************************************
"""
)
server.start()
