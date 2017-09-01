# Spark Shuffle

---

## 1. SortShuffleWriter

Shuffle中map端输出的数据要先写到磁盘，然后由reduce进行拉取。
> Hash Shuffle最早的是每个map-reduce对应一个文件，那么有n个map和m和reduce就会产生n*m个文件，由于可能产生很多小文件，每次打开文件和关闭文件会有很多开销，所以后面改进为合并成一个大文件。最初的优化是将所有小文件做一次合并，然后减少文件打开关闭的次数。

SortShuffleWriter的策略是，同一个map任务的shuffle块数据存储在一个 统一的**数据文件**中。数据块在数据文件中的偏移量存储在不同的**索引文件**中。那么n个map只会产生2n个文件。文件按照partition_id从小到大顺序写入。
Shuffle过程中map端的输出首先要依据partition ID进行排序。排序工作只能在内存中执行。如果排序的数据太多，内存中放不下，那么就将当前内存排序好的数据输出到磁盘文件。当所有shuffle数据都经过排序处理之后，对spill到磁盘的文件和当前内存中的数据进行合并。由于每个文件中每个partition的长度都已记录，而内存中的partition长度也有记录，所以merge过程如下。
```scala
for id <- 0 to partitions
then
  将每个spill文件和内存buffer中id对应的partition输出到合并文件
end
```
SortShuffleWriter和UnsafeShuffleWriter核心思想基本相同，只是排序数据存放的位置不同。SortShuffleWriter存放在堆内，UnsafeShuffleWriter存放在堆外。
## 2. UnsafeShuffleWriter

![UnsafeShuffleWriter原理图][1]

这种Shuffle策略的核心思想就是**将shuffle中待排序的数据放到堆外内存**，而堆内只存储堆外每条数据对应的地址。
和SortShuffleWriter不同的是，UnsafeShuffleWriter合并文件的操作是由本对象完成的（而非Sorter对象）。而且其有两种合并方式：1. **基于FileStream的方式**；2. **基于NIO TransferTo的方式**。由于输出文件可能采用压缩方式来减小文件大小（降低网络通信量），所以合并操作会面临解压的过程。基于FileStream的方式每轮会先减压spill文件对应的partition(reduce_id)，然后再写入到输出文件。而第二种方式通常要比第一种方式性能更好，其基于NIO的TransferTo，无需中间复制（很多操作系统支持直接从文件系统缓存直接传输到指定channel）。但是第二种方式需要文件的压缩方式和序列化方式支持，保证对原始数据的连接操作不会导致错误。

## 3. BypassMergeSortShuffleWriter
该策略就是过去的Hash Shuffle。核心思想就是每个map-reduce对应一个数据文件，然后根据配置将这些配置文件进行合并，最后每个map同样也是生成两个文件，一个数据文件，一个索引文件。
**对比**
1. 该策略适用于reduce数目较少的情况，文件数目较少的情况下，合并文件的速度还是比较快的。但是reduce数目太多，会导致文件的打开关闭操作过于频繁，从而降低性能。
2. 前两种策略的耗时操作在于排序，如果该策略可以避免排序。但是如果shuffle操作需要排序或者聚集操作，那么就应该直接选用前两种策略。




  [1]: http://on-img.com/chart_image/598fbfffe4b0b83fa25e3d89.png
