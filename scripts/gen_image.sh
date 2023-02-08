# 执行构建image命令，-t参数用来指定image文件的名字，后面还可以用:指定标签，如果不指定默认标签就是latest
# 最后的点儿 . 表示dockerfile文件所在路径，也就是当前路径。

docker image build -t lf:1.0.3 .