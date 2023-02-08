# -*- coding: utf-8 -*-
"""
@Time ： 2022/7/28 14:13
@Auth ： luanxing
@File ：http_tool.py
@IDE ：PyCharm
HTTP连接常用工具
"""

import requests
import traceback
from common.log_utils import get_logger

logger = get_logger(__name__)


def request_post(url, data, **param):
    """
    :param url: http地址
    :param param: body体, json格式
    :return:
    """
    try:
        headers = dict({'charset': 'utf-8', 'application': 'json'}, **param)
        response = requests.post(url, json=data, headers=headers, timeout=180)
        return response.json()
    except Exception as e:
        logger.error(traceback.format_exc())
        return {}


def request_get(url, param, **headers):
    try:
        response = requests.get(url, params=param, headers=headers)
        return response.json()
    except Exception as e:
        logger.error(e)
        return {}
