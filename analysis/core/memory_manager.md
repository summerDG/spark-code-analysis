#Spark的内存管理

Spark中的内存管理最近几个版本一直有变化。Spark1.6中将过去的内存管理全部放在了`StaticMemoryManager`中，其名字起得很好，因为过去的的内存
分配确实是静态的(详情见[Spark Architecture][1])，即各个区块的大小是固定的。可以通过配置`spark.memory.useLegacyMode`为`true`来使用。自从1.6版本开始就着力推出新的内存管理，这是[Tungtsen][2]中重要的一个目标。实际上1.6只是一个过渡，其和2.0.0的内存管理还是有一定的区别（在off-heap方面）。[1.6的内存管理见][3]。

##UnifiedMemoryManager Overview

新的内存管理策略在有这个类进行负责，这里借用[Alexey Grishchenko分析Spark1.6的内存管理][3]的图（有修改）来说明其整体的策略。

![Spark内存管理][Spark-Memory-Management-1.6.0]

1. Reserved Memory：
这部分空间大小是固定的（可以通过`spark.testing.reservedMemory`设置，需重新编译Spark），300MB。这意味着有300MB的内存不会用于Spark。这部分内存的目的是为了保证有
空间来运行其他系统，并且也可以用于限制设置的内存大小。所以如果你有1GB的JVM，那么用于“运行”（概念稍有不同，稍后解释）和“存储”的内存就是_(1024MB - 300MB) * spark.memory.storageFraction_。
此外，需要注意的是，Spark的Executor和Diver的内存大小至少要有_1.5 * Reserved Memory = 450MB_。否则就会提示通过`--driver-memory`或`--executor-memory`增加内存。
2. User Memory：
这部分空间是属于Spark管理的内存，其用于存储你自己的数据结构、Spark的内部元数据，以及对大而稀疏的数据规模的预测。令除去Reserved Memory之后
的内存为`usableMemory`，那么这部分的内存大小就是_usableMemory * (1 - spark.memory.storageFraction)_。
3. Spark Memory：
这部分内存的大小就是_usableMemory * spark.memory.storageFraction_。其共分为两部分一部分是Execution Memory和Storage Memory。虽然会设置`spark.memory.storageFraction`，但两者的边界并不是固定的，任何一部分内存都可以向另一部分去借内存。当然可以设置`spark.memory.offHeap.enabled`为`true`，并设置`spark.memory.offHeap.size`，即off-heap Memory的大小。这种配置下，在off-heap Memory上的Execution Memory和Storage Memory也一样遵循前文的策略。

* Execution Memory表示的是用于shuffles、join、sorts和 aggregations的内存。Execution Memory当然也可以向Storage Memory借空闲内存，当然也不是无限制地借，而是最多借到初始状况，也就是说Storage Memory最少也不会少于初始值。但是
Execution Memory的非空闲内存__永远都不会__被腾出来用于Storage Memory存储，所以Storage Memory借不到内存的时候就会把已存块按照存储层级踢出去（踢到磁盘或者直接不存）。

* Storage Memory表示用于persist（cache）、跨集群传递内部数据的内存，以及临时用于序列化数据“unroll”的内存。而且所有广播变量的数据
都作为cached blocks（存储层级为`MEMORY_AND_DISK`）存储在这里。当这部分内存不足的时候，它可以去向Execution Memory申请
__空闲__内存。但是当Execution Memory需要收回这部分内存的时候，要踢出部分cached blocks来满足Execution Memory的请求。

> 
1. Execution Memory <= _usableMemory * spark.memory.storageFraction_ * (1 - spark.memory.storageFraction)，但是它可以踢掉Storage Memory额外占用的它的那部分内存。
2. Storage Memory >= _usableMemory * spark.memory.storageFraction * spark.memory.storageFraction，但它无权踢掉Execution Memory的非空闲内存。

##Spark Tungtsen的内存思路

与C等直接面向内存的编程语言不同，Java业务逻辑操作内存是JVM堆内存，分配释放以及引用关系都由JVM进行管理，new返回的只是一个对象引用，而不是该对象在进程空间的绝对地址。但是由于堆内存的使用严重依赖JVM的GC器，对于大内存的使用，JavaER都想脱离JVM的管理，而自行和内存进行打交道，即堆外内存。

目前Java提供了ByteBuffer.allocateDirect函数可以分配堆外内存，但是分配大小受MaxDirectMemorySize配置限制。分配堆外内存另外一个方法就是通过Unsafe的allocateMemory函数，相比前者，它完全脱离了JVM限制，与传统C中的malloc功能一致。这两个函数还有另外一个区别：后者函数返回是进程空间的实际内存地址，而前者被ByteBuffer进行包装。

堆外内存使用高效，节约内存（基于字节，不需要存储繁琐的对象头等信息），堆内内存使用简单，但是对于Spark来说，很多地方会有大数组大内存的需求，内存高效是必须去追求的，它对整个程序运行性能影响极大，因此Spark也提供了堆外内存的支持，从而可以优化Spark运行性能。

对于堆内存，对象的引用为对象在堆中“头指针”，熟悉对象在堆中组织方式以后（比如对象头大小），就可以通过引用+Offset偏移量的方式来操作对象的成员变量；对于堆外内存，我们直接拥有的就是指针，基于Offset可以直接内存操作。通过这种机制，Spark利用MemoryLocation，MemoryBlock来对堆内和堆外内存进行抽象，以LongArray等数据结构对外提供统一的内存入口。类似下图。

![HashMap][new-memory-offheap]

这种设计可以极大地提高数组、Map等操作的效率。Tungtsen中已经利用这种设计替换了之前的原生数据结构。

##UnifiedMemoryManager源码分析

首先分析其父类MemoryManager

private[spark] abstract class MemoryManager(
    conf: SparkConf,
    numCores: Int,
    onHeapStorageMemory: Long,
    onHeapExecutionMemory: Long) extends Logging {

  @GuardedBy("this")
  protected val onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP)
  @GuardedBy("this")
  protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)
  ...
}

上面列举出其最重要的4个成员变量，分别是on-heap和off-heap版本的StorageMemoryPool和ExecutionMemoryPool。

###StorageMemoryPool

先分析StorageMemoryPool。

private[memory] class StorageMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {
  private[this] var _memoryUsed: Long = 0L
  private var _memoryStore: MemoryStore = _
}

`lock`的作用是同步对该变量的操作，同步是由MemoryManager来完成的。`memoryMode`指定了该内存池的属性，off-heap还是on-heap。
然后就是重要的两个成员变量：`_memoryUsed`和`_memoryStore`。前者表示该部分已经使用的内存，后者就是该内存池对应的用于存储操作的接口（[Spark Block Manager管理][4]有介绍）。

这里着重介绍`_memoryStore`，它的目的就是用于必要时将某些块踢到磁盘。它的调用点只有一个就是`acquireMemory`。如果当前的
空闲内存不够的时候，就会调用`memoryStore.evictBlocksToFreeSpace`来踢cached blocks。这个函数主要是将满足条件的块踢出去，
条件有两点：一是准备存储的块和被踢块的存储层级相同；而是一个块不能替换本RDD中的块。如果被选中的满足条件的块的总大小已经满足替换空间的要求，就停止查找，然后去调用`dropBlock`函数将对应BlockId的块踢出去，这个函数具体时间调用了`blockEvictionHandler`变量的
`dropFromMemory`，而该函数就是在BlockManager中实现的（因为BlockManager是BlockEvictionHandler的子类）。该函数中首先会去查看这个块是否使用了`useDisk`这个属性，如果有就先将序列化后的数据写到磁盘上（已经序列化的就直接写到磁盘）。然后就是利用MemoryStore的`remove`函数释放对应块的存储空间并且通知更新块信息。下面看`remove`函数。中会通过MemoryManager的`releaseStorageMemory`释放空间（仅仅是改变`_memoryUsed`的大小），真正从物理上释放空间的操作。

	//MemoryStore
	def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
		val entry = entries.synchronized {
		  entries.remove(blockId)
		}
		if (entry != null) {
		  entry match {
			case SerializedMemoryEntry(buffer, _, _) => buffer.dispose()
			case _ =>
		  }
		  memoryManager.releaseStorageMemory(entry.size, entry.memoryMode)
		  logDebug(s"Block $blockId of size ${entry.size} dropped " +
			s"from memory (free ${maxMemory - blocksMemoryUsed})")
		  true
		} else {
		  false
		}
	}

其中会通过MemoryManager的`releaseStorageMemory`释放空间（仅仅是改变`_memoryUsed`的大小），真正从物理上释放空间的操作是
`buffer.dispose()`，`buffer`是一个ChunkedByteBuffer类型的对象。ChunkedByteBuffer是可以看做是一组ByteBuffer，但每个ByteBuffer
的offset必须为0，由于是只读的，所以这组ByteBuffer的数据只能通过copy来供调用者使用。

	//ChunkedByteBuffer
	def dispose(): Unit = {
		if (!disposed) {
		  chunks.foreach(StorageUtils.dispose)
		  disposed = true
		}
	}
	//StorageUtils
	def dispose(buffer: ByteBuffer): Unit = {
		if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
		  logTrace(s"Unmapping $buffer")
		  if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
			buffer.asInstanceOf[DirectBuffer].cleaner().clean()
		  }
		}
	}
	
可以发现最终会调用Cleaner的`clean`方法来释放这部分空间（sun.misc）。不过至此我们还没有分析怎么向off-heap中写数据，实际上向off-heap中写数据只存在于MemoryStore的`putIteratorAsBytes`中，进一步往回找，发现只有在BlockManager的`doPutIterator`中hi调用此函数，而且该函数只会被
BlockManager的`getOrElseUpdate`调用，也就是触发persist（cache）操作的时候。所以off-heap只会用于persist操作。

###ExecutionMemoryPool

接下来分析ExecutionMemoryPool，该类除了像StorageMemoryPool存储数据（只是用途不同），还提供了一组策略来保证每个任务都可以得到一部分
合理的内存。

假设有N个任务，它保证每个任务在溢出之前有至少1/2N的内存，至多1/N的内存，就是说每个任务的内存持有量是[1/N, 1/2N]。
由于N是动态变化的，所以会一直跟踪活跃任务，重新在等待任务中计算1/2N和1/N。

任务会向ExecutionMemoryPool竞争发起申请。针对为每个任务，ExecutionMemoryPool都记录了它们当前申请的内存大小，同时在申请过程中，为了保证task分配的内存总大小位于[1/2N,1/N]之间，如果可申请大小达不到1/2N，将会阻塞申请（让锁等待），等待其他task释放相应的内存。这部分代码在ExecutionMemoryPool的`acquireMemory`中。ExecutionMemoryPool中还有一个重要的方法就是`releaseMemory`，就是释放对应任务的指定大小的内存。这两个函数的代码很容易懂，所以不贴了。

如果想全面理解ExecutionMemoryPool的原理，对Spark-Shuffle到深度理解是很有必要（见[Spark基础及Shuffle实现][5]和[Spark的shuffle机制分析][6]）。

ExecutionMemoryPool，只是维护一个可用内存指标，接受指标的申请与回收，实际负责内存管理的是TaskMemoryManager，它的工作单位是Task，即一个Executor里面会有多个TaskMemoryManager。
他们共同引用一个ExecutionMemoryPool，以竞争的方式申请可用的内存指标，申请指标的主体被表示为MemoryConsumer，即内存的消费者。在[Spark的shuffle机制分析][6]中提到“Deserialized sorting”和“serialized sorting”两种Sorter，其都属于MemoryConsumer。它核心的功能就是支持内存的申请以及在内存不够的时候,可以被动的进行Spill，从而释放自己占用的内存。因此两种Sort支持插入新的数据，也支持将已经Sorter数据Spill到磁盘。

具体来说，是每个任务有一个TaskMemoryManager变量，对于ShuffleMapTask来说，有ShuffleWriter，其中UnsafeShuffleWriter和SortShuffleWriter，
这二者都会包含ExternalSorter类型变量`sorter`，写数据的时候会调用该变量的`insertRecordIntoSorter`方法，然后调用`insertRecord`，
该函数中会判断是否需要spill（没有足够内存用于该任务），如果是就调用MemoryConsumer的`spill`方法将一些数据spill到到磁盘来释放内存。

了解了Shuffle与ExecutionMemoryPool的关系之后，现在分析一下TaskMemoryManager。

####TaskMemoryManager

这个类很复杂，其中大部分是将off-heap地址转换成64-bit的long型。在off-heap模式下，内存可以直接用64-bit的long值处理。
在on-heap模式下，内存可以通过对象引用和一个64-bit的long值的offset来处理（最初想法）。当想存储其他数据结构中包含的数据结构指针时，例如hashmap或已排序buffer，那么这种方式就会有问题，因为地址完全可以大于这个范围。所以对于on-heap模式，使用64-bit中的高13位来存储页好，低51位来存储页内偏移量（offset）。页号被存储在TaskMemoryManager的页表数组中。所以允许存储_2 ^ 13 = 8192_页，页大小受限于long[]数组（最大2^31），所以可以处理_8192 * 2^31 * 8 bytes_的数据，16TB内存。

分析TaskMemoryManager的页表数组`pageTable`，MemoryBlock[]数组，先来分析MemoryBlock和MemoryLocation。

	public class MemoryLocation {
	  @Nullable
	  Object obj;
	  long offset;
	}
	public class MemoryBlock extends MemoryLocation {

	  private final long length;

	  public int pageNumber = -1;

	  public static MemoryBlock fromLongArray(final long[] array) {
		return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8L);
	  }

	  public void fill(byte value) {
		Platform.setMemory(obj, offset, length, value);
	  }
	}
	
MemoryLocation表示内存位置（on-heap模式），只有两个成员变量，就是对象引用和offset。MemoryBlock（省略了构造函数）是一块连续的内存，从
MemoryLocation位置开始，长度为`length`。其还包含该段内存的页号。由`fromLongArray`可以了解到如何将long[]数组转化为
MemoryBlock，Platform.LONG_ARRAY_OFFSET表示了数组的起始位置（因为Java对象和C++不同，会有头信息），数组所存数据总长度就是_个数 * 64bit_。填充这块内存的操作类似于C语言。

下面来看一些有意思的成员变量。`allocatedPages`，用一个BitSet来表示空闲页（为什么用BitSet，我想应该是，首先页号是连续的，所以BitSet不会造成太大的空间浪费，其次就是BitSet查询插入很快）。`tungstenMemoryMode`就是用于判断off-heap和on-heap，因为二者的内存寻址不同。
`consumers`就是前面提到的Sorter，但是这里为什么是一个HashSet，每个任务不应该只有一个么？其实这里的`consumers`并不是指这个
TaskMemoryManager对应的`consumers`，而是向其申请内存的`consumers`。`acquiredButNotUsed`表示申请了但没有使用的的内存。

来关注几个比较重要的方法。

	//TaskMemoryManager
	public long acquireExecutionMemory(long required, MemoryConsumer consumer) {
		...
		MemoryMode mode = consumer.getMode();

		synchronized (this) {
		  long got = memoryManager.acquireExecutionMemory(required, taskAttemptId, mode);

		  if (got < required) {
			// Call spill() on other consumers to release memory
			for (MemoryConsumer c: consumers) {
			  if (c != consumer && c.getUsed() > 0 && c.getMode() == mode) {
				try {
				  long released = c.spill(required - got, consumer);
				  if (released > 0) {
					...
					got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, mode);
					if (got >= required) {
					  break;
					}
				  }
				} catch (IOException e) {
				  ...
				}
			  }
			}
		  }

		  // call spill() on itself
		  if (got < required) {
			try {
			  long released = consumer.spill(required - got, consumer);
			  if (released > 0) {
				...
				got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, mode);
			  }
			} catch (IOException e) {
			  ...
			}
		  }

		  consumers.add(consumer);
		  return got;
		}
	}
	public MemoryBlock allocatePage(long size, MemoryConsumer consumer) {
		...
		long acquired = acquireExecutionMemory(size, consumer);
		if (acquired <= 0) {
		  return null;
		}

		final int pageNumber;
		synchronized (this) {
		  pageNumber = allocatedPages.nextClearBit(0);
		  if (pageNumber >= PAGE_TABLE_SIZE) {
			releaseExecutionMemory(acquired, consumer);
		  }
		  allocatedPages.set(pageNumber);
		}
		MemoryBlock page = null;
		try {
		  page = memoryManager.tungstenMemoryAllocator().allocate(acquired);
		} catch (OutOfMemoryError e) {
		  ...
		}
		page.pageNumber = pageNumber;
		pageTable[pageNumber] = page;

		return page;
	}

为了方便理解，删去大量细节代码。首先来看`allocatePage`，MemoryConsumer申请一定大小的空闲页（申请大小不能大于最大页长）。
其实还是调用`acquireExecutionMemory`来申请特定大小内存，当然页号也不能超出最大页号。关于Allocator在[Spark Block Manager管理][4]中
略微提到。针对on-heap和off-heap有不同的Allocator，其实这里只是利用Allocator进行生成页的信息（页号、起始位置和长度）。

然后再来看`acquireExecutionMemory`，首先就是调用`MemoryManager`的`acquireExecutionMemory`来申请内存，这个函数之前没讲过，但是和
`acquireStorageMemory`总体类似，区别在于申请StorageMemory的时候不可以踢ExecutionMemory，但申请ExecutionMemory的时候可以踢掉__超出`storageRegionSize`的StorageMemory__。但加入申请到的内存不能满足需求，就需要将其他借过内存的`consumer`的内存数据spill到磁盘。那么释放出来的空间就可以供这个`consumer`使用，如果这种情况还不够使用，那就只能spill本任务的内存数据到磁盘了（一种妥协策略，就是尽量满足当前使用，假设该任务之前申请到的内存存得已经是冷数据了）。
最后就是把这个`consumer`加入列表，说明其曾经向本任务请求过内存，必要的时候向他们**讨还**内存（谁找我借的，我就找谁要）。

释放页和释放内存的操作类似（像是C语言或者C++风格）。

内存已经分配了，那怎么想这块内存中写数据呢？可以去看Spark自己实现的LongArray（org.apache.spark.unsafe.array），构造函数就是传入一个
MemoryBlock，也就是一页，因为页中有偏移量，长度，所以可以按序写入（利用Platform的写操作），并且不会越界。

剩下的就是`encodePageNumberAndOffset`、`decodePageNumber`、`decodeOffset`，这几个很容易理解。`encodePageNumberAndOffset`是针对on-heap和off-heap中的页和页内偏移进行编码的。`getPage`是获得页的引用（只针对on-heap有效，因为off-heap并没有页引用）。这些函数在UnsafeExternalSorter和新数据结构中有大量应用，这里不再赘述。

##JIT编译

实际上Tungtsen借用`sun.misc.Unsafe`管理内存后，其内存操作（申请、插入、释放）都是原生的，即直接通过JIT编译编译成机器指令，而无需JVM将 Java字节码解释后再运行。所以执行速度也会有提升。
	
[1]:https://0x0fff.com/spark-architecture/
[2]:https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html
[3]:https://0x0fff.com/spark-memory-management/
[4]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/block_manager.md
[5]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/spark_shuffle.md
[6]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/spark_sort_shuffle.md
[Spark-Memory-Management-1.6.0]:../pic/Spark-Memory-Management-1.6.0.png
[new-memory-offheap]:../pic/new-memory-offheap.png
