#!/usr/bin/python
#-*-coding:utf-8-*-

DB:
  cmdexecution
    {
        "target_id": ...,
        "scan_id": ...,
        "job_id": ...,
        "count": ...,
        [
            {},
            {}
        ]
    }
------------------------------

"cmdexecution" : # 命令执行
{
    "addr": "漏洞地址",
    "match_words": "匹配字符",
    "method": "提交方法",
    "params": {参数字典}
}

"filefuzzing": # 敏感文件
"sourceleek":  # 源码泄漏
"xssscan":     # xss
{
    "addr": "敏感文件路径",
    "method": "提交方法",
    "params": {参数字典}
}

"fileinclusion": # 文件包含
{
    "language": "php",
    "addr": "漏洞地址",
    "type": "文件包含类型"
    "remote_injectable": BOOL,
    "readable_file": [文件列表],
    "header_sent": "",
    "path": "",
    "get_param": "",
    "os": ""
}

"loginform": # 登录框
{
    "addr": "",
    "method": "",
    "params": {参数字典}
}

##################################################################

"siteinfo" :
{
    "target_id": ...,
    "scan_id": ...,
    "job_id": ...,
    "vuln_names": [],

    "paddingoracle": {"vulnerable": BOOL},
    "platform": {
        "ssl": {"vulnerable": BOOL},
        "struts2": {"vulnberable": BOOL, "addr": "..."},
    },
    "subdomain": [],
    "sidesite": [],
    "waf": {"name": "..."},
    "portservice": [],
    "whois": {           # whois信息
        "create_time": "",
        "domain_status": [],
        "dns_server": [],
        ...
    },
    "cms": {             # cms类型
        "cms_type": [""],
        "maybecms": ["", ...]
    },
    "middleware": [       # 中间件
        {
            "name": "...",
            "info": {
                "server_status": ...
            }
        }
    ]
}

