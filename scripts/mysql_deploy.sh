# 用于部署mysql

sudo docker run --name sfa_mysql \
-itd -p 8165:3306 \
-e MYSQL_ROOT_PASSWORD=123456 \
--restart=always \
-e TZ=Asis/Shanghai \
-v /home/luanxing/docker/sfa_mysql/my.cnf:/etc/mysql/my.cnf \
-v /home/luanxing/docker/sfa_mysql/data:/var/lib/mysql \
mysql:5.7 \
--character-set-server=utf8mb4 \
--collation-server=utf8mb4_unicode_ci \
--character-set-client-handshake=FALSE