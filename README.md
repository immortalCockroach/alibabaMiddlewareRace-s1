阿里巴巴中间件大赛第一赛季代码
===
[比赛地址](https://tianchi.shuju.aliyun.com/programming/information.htm?spm=5176.100067.5678.2.pMusAv&raceId=231533 "比赛地址")

***
* 第一赛季排名**33**,准确率为**100**,耗时为**565377**(有点长)。拓扑为非常简单的线性拓扑，其中启用了**ACK机制**和流量控制等措施。
* 曾经尝试过高级一点的非线性拓扑，但是都失败了。
* 优化的几个点主要是**增量写入**和多线程下的**同步锁**
* 注意的坑是jstorm中多个task可能分布在不多的jvm进程中，如果数据有依赖并且分布在不同的jvm中的话，就会出现互相无法查找到的情况 
* 下面给出几个比较有用的链接
  1.[jstorm中Worker、Executor、task的区别](http://www.cnblogs.com/Jack47/p/understanding_the_parallelism_of_a_storm_topology.html)
  2.[jstorm中grouping的方式](http://san-yun.iteye.com/blog/2095475)
  3.[jstorm中流的分离和聚合](http://shiyanjun.cn/archives/977.html)
  4.[jsotrm中ack机制的原理](http://blog.csdn.net/derekjiang/article/details/9047443)