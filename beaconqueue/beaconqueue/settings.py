#!/usr/bin/python
#-*-coding:utf-8-*-
class MongoAddr:
    host = "172.16.4.88"
    port = 30000


class MongoClientConfig:
    # 发送正常请求(non-monitoring)后多长时间没有响应判定为网络错误, 默认 None - 无超时
    socketTimeoutMS = 5000
    
    # 判定无法连接服务器的超时时间
    connectTimeoutMS = 5000
    
    # serverSelectionTimeoutMS = None
    # waitQueueTimeoutMS = None
    # waitQueueMultiple = None
    

class RpcConfig:
    host = "127.0.0.1"
    port = 7777
