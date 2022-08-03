# z_kv

目前kv引擎相关的知识繁杂，自己动手从0实现一个KV存储引擎不失为一种很好的学习路线

本项目虽以学习为目的，但是仍以工业级应用为目标，实现用于持久化存储用户日志信息。语言为C++，平台为linux。

内存结构：实现了跳表，分片式LRU缓存器，基于空闲列表的内存池，布隆过滤器

磁盘结构：采用LSM-tree模式，设计了sst文件结构，协程负责并行压缩，设计了运行日志负责崩溃后的数据恢复

优化：批量写提高写入效率，针对大value进行kv分离，垃圾回收机制


to do:

性能优化

hotring

单机事务
