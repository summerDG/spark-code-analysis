#Spark的内存管理

Spark中的内存管理最近几个版本一直有变化。Spark1.6中将过去的内存管理全部放在了`StaticMemoryManager`中，其名字起得很好，因为过去的的内存
分配确实是静态的(详情见[Spark Architecture][1])，即各个区块的大小是固定的。可以通过配置`spark.memory.useLegacyMode`为`true`来使用。自从1.6
版本开始就着力推出新的内存管理，这是[Tungtsen][2]中重要的一个目标。实际上1.6只是一个过渡，其和2.0.0的内存管理还是有一定的区别。[1.6的内存管理
见][3]。

##UnifiedMemoryManager介绍

新的内存管理策略在有这个类进行负责，这里借用[Alexey Grishchenko分析Spark1.6的内存管理][3]的图来说明其整体的策略。

![Spark内存管理][4]

###Reserved Memory

这部分空间大小是固定的，300MB。

[1]:https://0x0fff.com/spark-architecture/
[2]:https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html
[3]:https://0x0fff.com/spark-memory-management/
[4]:../pic/Spark-Memory-Management-1.6.0.png
