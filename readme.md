# 基于ＲＤＭＡ的阵列数据库网络模块优化

本项目是在开源阵列数据库SciDB(现在已经闭源，使用的是以前开源的19.11版本)基础之上，在网络模块中添加了RDMA模块，可以在具有RDMA网络的环境下使用，提升SciDB的查询性能。

## 使用步骤：
1. 按照SciDB的安装文档，完成SciDB依赖模块的安装。
2. 新增了Muduo开源库依赖，需要安装Muduo，Muduo项目地址：[Muduo](https://github.com/chenshuo/muduo)。
3. 配置文件config.ini中的use-rdma修改为true即可使用RDMA网络直接发送数据。
4. 启动集群即可。

## 修改的代码

新增的代码主要在网络模块src/network/rdma目录下面，另外还修改了entry.cpp，connection.cpp等文件。