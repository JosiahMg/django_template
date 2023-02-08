from enum import Enum


class Status(Enum):
    SUCCESS = (200, 'success')
    BAD_POST = (405, '不支持POST请求')
    BAD_GET = (405, '不支持GET请求')
    PARAM_ERROR_LACK = (401, '参数校验失败,缺少必要参数')
    PARAM_ERROR_INVALID = (401, '参数校验失败,非法值')
    OTHER_ERROR = (400, '未知错误')


# 初始返回状态
INIT_STATUS = {
    'statusCode': 200,
    'message': 'success'
}

STATUS = {item.name: {'statusCode': item.value[0], 'message': item.value[1]} for item in Status}
