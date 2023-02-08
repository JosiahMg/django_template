import os

from conf.path_config import project_dir

# fp为你想要计算的根目录
fp = os.path.join(project_dir)
# fp = os.path.join(project_dir, '_deploy_packages', 'LF')
globals()['total_lines'] = 0
globals()['total_files'] = 0
calculate_list = ['py', 'yaml', 'md', 'txt', 'ini', 'env', 'cfg']


def recursion_search_dir(fp, cur_path):
    if os.path.isdir(fp):
        # print('----', fp)
        cur_files = os.listdir(fp)
        for fn in cur_files:
            # print(fn)
            if '.' not in fn:  # 目录
                recursion_search_dir(fp + '\\' + fn, cur_path=cur_path + fn + '/')
            else:  # 文件
                local_path = fp + '\\' + fn
                if fn.split('.')[-1] in calculate_list:
                    globals()['total_files'] += 1
                    try:
                        with open(local_path, 'r', encoding='utf-8') as f:
                            globals()['total_lines'] += len(f.readlines())
                    except UnicodeDecodeError:
                        with open(local_path, 'r', encoding='gbk') as f:
                            globals()['total_lines'] += len(f.readlines())


recursion_search_dir(fp, '')
print(globals()['total_files'])
print(globals()['total_lines'])
