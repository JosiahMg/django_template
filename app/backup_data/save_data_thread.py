# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/12/2 14:15
# Function: 多线程保存文件
import os
import threading

from common.log_utils import get_logger
from conf.path_config import resource_dir

logger = get_logger(__name__)


class SaveDataThread(threading.Thread):
    def __init__(self, project_id, batch_no, data, data_type):
        super(SaveDataThread, self).__init__()
        self.project_id = project_id
        self.batch_no = batch_no
        self.data = data
        self.data_type = data_type

    def run(self):
        save_path = os.path.join(resource_dir, self.project_id, self.batch_no)
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        self.data.to_csv(os.path.join(save_path, f"{self.data_type}.csv"), index=False)
        logger.info(f"thread save {self.data_type} success.")


class SaveAllPipeResultTread(threading.Thread):
    """ 多线程保存batchNo对应的pipe result数据 """

    def __init__(self, project_id, batch_no, td_driver):
        super(SaveAllPipeResultTread, self).__init__()
        self.project_id = project_id
        self.batch_no = batch_no
        self.td_driver = td_driver

    def run(self):
        save_path = os.path.join(resource_dir, self.project_id, self.batch_no)
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        ts_df = self.td_driver.load_compute_result()
        ts_df.to_csv(os.path.join(save_path, "pipe_results.csv"), index=False)
        logger.info(f"thread backup batch_no: {self.batch_no} pipe result data.")
