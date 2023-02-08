# 初始脚本
cd docs
sphinx-quickstart

# 初始设置
zh_CN # 语言

# conf设置
import os
import sys
import sphinx_rtd_theme
sys.path.insert(0, os.path.abspath('./../..'))
sys.path.insert(1, os.path.abspath('./../../app/'))
sys.path.insert(2, os.path.abspath('./../../api/'))
sys.path.insert(3, os.path.abspath('./../../common/'))

# 生成rst文件
sphinx-apidoc -o ./source ../app/

# 在build中生成html文件
make html
