# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/11/10 17:56
# Function:  气源追踪
import os.path
from datetime import datetime

from app.trace_source.trace_source import TraceSource
from common.log_utils import get_logger
from conf.path_config import data_dir

logger = get_logger(__name__)


class ServiceSourceTrace:
    def __init__(self, info_dict):
        self.info_dict = info_dict
        self.check_param()
        self.project_id = info_dict['projectId']
        self.topo_id = info_dict['topologyId']
        self.batch_no = info_dict['batchNo']
        self.calc_type = info_dict.get('businessType', "single")    # single: 单独计算    continue: 连续计算
        self.start_time = info_dict.get("startTime", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.data_path = os.path.join(data_dir, self.project_id)
        logger.info(
            f"trace source project_id: {self.project_id}, batch_no: {self.batch_no}, start_time: {self.start_time}")

    def execute(self):
        TraceSource(self.project_id, self.topo_id, self.batch_no, self.start_time, self.calc_type).execute()

    def check_param(self):
        """ 检查数据的合法性
        """
        if not self.info_dict.get("projectId", ""):
            logger.error("projectId parameter invalid on trace source")
            raise Exception("projectId 参数无效")

        if self.info_dict.get("startTime", ""):
            dt = datetime.strptime(self.info_dict["startTime"], "%Y-%m-%d %H:%M:%S")
            if dt > datetime.now():
                logger.error("start time is future time")
                raise Exception("startTime的时间是未来的时间")
            else:
                # 防止传递的字符串格式错误 因此需要重写标准化该字符串时间格式
                self.info_dict["startTime"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            logger.error("start time is null")
            raise Exception("startTime为空")

    @staticmethod
    def standard_gis_ids(gis_ids):
        gis_ids = gis_ids if isinstance(gis_ids, list) else [gis_ids]
        gis_ids = list(map(str, gis_ids))
        return gis_ids
