#!/usr/bin/python
#-*-coding:utf-8-*-
import rpyc
import json

class QueueClient(object):
    def __init__(self, queue_server_addr, queue_server_port):
        self._addr = queue_server_addr
        self._port = queue_server_port
        self.__conn = None

    @property
    def _conn(self):
        """
        返回到 QueueServer 的 rpyc 连接
        """
        if not self.__conn or self.__conn.closed:
            self.__conn = rpyc.connect(
                self._addr, self._port,
                config = {"allow_public_attrs" : True}
            )
        return self.__conn

    def register(self, job_type, job_kwargs, job_identifiers, priority=100):
        """
        :param job_type: <str> 任务类型名
        :param job_kwargs: <dict> 任务参数表
        :param job_identifiers: <dict> 任务标识符键值列表,
            一般有 target_id, scan_id

        :return:
          1. 成功: {"result": True, "status": "OK"}
          2. 失败: {"action": "...", "error": "错误名称", ...}
        """
        return self._conn.root.register(
            job_type, job_kwargs, job_identifiers, priority
        )

    def fetch(self, job_type, by_who, num=1):
        """
        :param job_type: <str> 任务类型名
        :param by_who: <str> 谁(哪个节点)抓走了该任务
        :param num: <int> 获取任务的个数

        :return:
          1. 成功: 任务信息的字典列表
          2. 失败: {"action": "...", "error": "错误名称", ...}
        """
        return self._conn.root.fetch(
            job_type, by_who, num=1)

    def finishing(self, job_id, result_addr,
                  job_identifier_names=["target_id", "scan_id"]):
        """
        :param job_id: <str> 任务id
        :param result_addr: <str> 该任务查询地址(使用json数据)
        :param job_identifier_names: <list> 任务中作为标识符的字段

        :return:
          1. 操作成功且任务存在:  {'result': True, 'status': 'OK'}
          2. 操作成功但任务不存在: {'result': None, 'status': 'OK'}
          3. 
        """
        return self._conn.root.finishing(
            job_id, json.dumps(result_addr), job_identifier_names
        )

    def inspect(self, which_queue, condition,
                return_count=True, return_fields={}):
        """
        :param which_queue: <enum QUEUE_DEFS> 查看哪张表的状态
        :param condition: <dict> 查询条件
        :param return_count: <bool> 为True, 返回符合查询条件的条目数量
        :param return_fields: <dict> 若return_count为False, 返回该参数中值为True的字段的值

        :return:
          1. return_count = True
            a. 成功: {"result": 符合条件的条目数量, "status": "OK"}
          2. return_count = False
            a. 成功: {"result": 符合条件的return_fields指定字段返回结果字典的列表,
                      "status": "OK"}
        """
        return self._conn.root.inspect(
            which_queue, condition,
            return_count, return_fields
        )
