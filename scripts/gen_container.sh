docker run -p 8100:8080 --name intel_networks -dit intel_networks:v1.0.1 /bin/bash
docker run -p 18059:18059 --name lf -dit lf:1.0.3
# -p 参数：容器的8080端口映射到本机的8000端口
# -it 参数：容器的shell映射到当前的shell，在本机窗口输入命令，就会传入容器
# -d 参数：在后台运行程序
# django-docker 参数： image文件的名字

docker run -it -d --name=intel_networks -p 8176:8080 --link=e26aff30985b re_image:1.0