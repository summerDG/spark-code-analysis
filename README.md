#Spark源码分析

该专题主要分为两个大的章节，第一章是关于spark core的源码分析，第二章是关于spark sql的源码分析。

##spark core

该章主要从两个角度分析了spark core的源码：计算和存储。

###计算

计算方面涉及到的内容主要包含：作业及任务的调度和shuffle。这里默认读者已经了解了RDD的概念和原理，所以并未对RDD作分析。

[Spark基础知识][1]:该节主要介绍了Job，Stage，Task，Dependency等概念，并且从一个简单实例入手，阐述了这些概念间的关系。主要对Job的调度做了分析。最后简单介绍了Shuffle过程中涉及到的概念和流程。

[Spark的Shuffle机制][2]：该节主要分析了Spark中各种Shuffle的实现过程。

[Spark的任务调度机制][3]：该节介绍的是Task的调度机制，包括核心概念和实现流程。

###存储

存储管理分两节讲解，第一节是从用户角度，即数据的存储单位Block的角度进行阐述，第二节是从实现方式的角度，即内存管理来阐述。

[Spark Block管理][4]：对于很对涉及到与存储打交道的操作（如Shuffle、broadcast等）来说，都会涉及到BlockManager。本节主要对BlockManager及相关概念进行分析。

[Spark内存管理][5]：BlockManager落实到具体的存储（内存，磁盘等）方面是利用MemoryManager来完成的。所以该节是对Spark的内存管理的实现进行分析。

##spark sql

该章分为两部分：Catalyst分析和Join详解。

###Catalyst分析

这部分是完成对Catalyst的分析，从sql语句的解析到logical前半部分是完成对Catalyst的分析，从sql语句的解析到logical plan，再到logical plan优化，然后是physical plan，最后是执行。如下图所示。

![Catalyst实现][Catalyst]

[Spark SQL 基础知识][7]：本节主要是对Spark中涉及到的几个关键概念进行介绍，包括Row，Expression，Attribute，QueryPlan和Tree。

[Spark Catalyst分析阶段][6]：该节从sql语句开始进行分析，所以会涉及到sql语句解析和生成LogicalPlan的内容。

[Catalyst Logical Plan优化][8]：本节是在上一节的基础上对LogicalPlan进行优化，主要分析了部分优化方案。

[Spark SQL Physical Plan][9]：本节是对优化后的Logical Plan进行处理，生成用于执行的Physical Plan。

[Spark SQL 执行阶段][10]：在PhysicalPlan生成之后，就是执行阶段，执行的时候会涉及到部分的优化方案，并且本节对DataSet的原理进行了分析。

###Join详解

这部分主要是针对Join算子的分析，前半部分的分析涵盖基本的处理思路，但是并不会细致到每个算子都分析，如果相分析具体算子可以沿着前半部分的思路去挖掘。

[Spark SQL Join分析——上][11]：针对具体的Join算子，分析了从SQL语句的AST树解析到LogicalPlan优化完成，也就是生成PhysicalPlan之前，的代码。

[Spark SQL Join分析——下][12]：针对具体的Join算子，从LogicalPlan到Spark core中具体的shuffle操作之前做了分析。

[1]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/core/spark_shuffle.md
[2]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/core/spark_sort_shuffle.md
[3]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/core/task_schedule.md
[4]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/core/block_manager.md
[5]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/core/memory_manager.md
[6]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_parser.md
[7]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_preparation.md
[8]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_optimize.md
[9]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_physicalplan.md
[10]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_execution.md
[11]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_join_1.md
[12]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_join_2.md
[Catalyst]:pic/Catalyst-Optimizer-diagram.png
