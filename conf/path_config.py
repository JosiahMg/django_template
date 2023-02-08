import os

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

log_dir = os.path.join(project_dir, 'logs')

resource_dir = os.path.join(project_dir, 'resource')
flights_dir = os.path.join(resource_dir, 'flights')

"""-----------------------结果输出路径---------------------------"""
data_dir = os.path.join(project_dir, 'data')
save_scada_dir = os.path.join(resource_dir, 'scada')
save_topology_dir = os.path.join(resource_dir, 'topology')

model_dir = os.path.join(project_dir, 'models')

