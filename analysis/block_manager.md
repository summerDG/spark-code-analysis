#Spark Block Manager管理

Spark中的RDD-Cache、broadcast、ShuffleWriter和ExternalSorter等都是基于BlockManager实现的。BlockManager会运行在每个节点上，
包括driver和executor。其主要是提供接口来检索各种存储（memory，disk和off-heap）中本地或远程的block。

##BlockManager创建

BlockManager是在SparkEnv中创建生成的。这里插一句SparkContext和SparkEnv的区别，前者是创建了整个Spark系统的环境变量或者调度器，
而后者是针对每个运行的Spark实体（master或worker）创建了运行时环境对象（如序列化器、RpcEnv、block manager、map output tracker等）。
当然SparkEnv也是在SparkContext中的`create`中创建的。

这里的Block和HDFS中谈到的Block块是有本质区别：HDFS中是对大文件进行分Block进行存储，Block大小固定为512M等；而Spark中的Block是用户
的操作单位， 一个Block对应一块有组织的内存，一个完整的文件或文件的区间端，并没有固定每个Block大小的做法。

	private[spark] class BlockManager(
		executorId: String,
		rpcEnv: RpcEnv,
		val master: BlockManagerMaster,
		serializerManager: SerializerManager,
		val conf: SparkConf,
		memoryManager: MemoryManager,
		mapOutputTracker: MapOutputTracker,
		shuffleManager: ShuffleManager,
		val blockTransferService: BlockTransferService,
		securityManager: SecurityManager,
		numUsableCores: Int)
	  extends BlockDataManager with BlockEvictionHandler with Logging {
	}
	
看BlockManager的构造函数，可以发现其主要包含了：executorId，rpcEnv，blockManagerMaster，serializerManager，SparkConf，
[memoryManager][3]，[mapOutputTracker][1]，[shuffleManager][2]，blockTransferService（用于一次拉取一组blocks），
securityManager，numUsableCores（可用核数）。

##Block相关知识

上面谈到，Block是用户的操作单位，而这个操作对应的key就是这里BlockID，value内容为ManagerBuffer。向看一下BlockDataManager
这个特质（接口），BlockManager就继承了它。它对外提供了对Block的操作，获取或者添加。

	trait BlockDataManager {
	  def getBlockData(blockId: BlockId): ManagedBuffer

	  def putBlockData(
		  blockId: BlockId,
		  data: ManagedBuffer,
		  level: StorageLevel,
		  classTag: ClassTag[_]): Boolean

	  def releaseLock(blockId: BlockId): Unit
	}

`getBlockData`是从BlockManager中获取对应Id的ManagerBuffer，`putBlockData`是将一个ManagerBuffer机器Id按照存储层级添加到
BlockManager中。

*BlockId*其本质上来说就是一个字符串。只是针对不同的Block命名方式不同。*ManagerBuffer*实际上是对ByteBuffer的封装。

	public abstract class ManagedBuffer {

	  public abstract long size();
	  
	  public abstract ByteBuffer nioByteBuffer() throws IOException;
	  
	  public abstract InputStream createInputStream() throws IOException;

	  public abstract ManagedBuffer retain();
	  
	  public abstract ManagedBuffer release();
	  
	  public abstract Object convertToNetty() throws IOException;
	}

由于Buffer不能长久存在于内存中不进行释放，所以这里有`retain`和`release`两个方法分别用于添加或释放对该Buffer的引用数目。对该Buffer
的对外访问接口就是`createInputStream`和`nioByteBuffer`，前者是将buffer中的数据作为InputStream暴露出来，后者是作为
NIO ByteBuffer暴露。注意这个nioByteBuffer函数是每次调用将会返回一个新的ByteBuffer,对它的操作不影响 真实的Buffer的offset和long。
这个接口有3个实现，FileSegmentManagedBuffer、NettyManagedBuffer、NioManagedBuffer和BlockManagerManagedBuffer，由类名就可以知道，区别在于
获取ByteBuffer的来源不同，FileSegmentManagedBuffer是保存了一个File类型的变量，所以是读取`file`里的内容生成ByeBuffer，
NettyManagedBuffer是通过ByteBuf读取的，NioManagedBuffer保存的直接就是ByteBuffer，BlockManagerManagedBuffer是通过ChunkedByteBuffer（spark.util.io）。

##Block状态维护

首先来看StorageLevel，在Spark中，对应RDD的cache有很多level选择，这里谈到的StorageLevel就是这块内容。首先我们来看存储的级别：

* DISK，即文件存储。
* Memory，内存存储,这里的内存指的是Jvm中的堆内存,即onHeap。
* OffHeap，非JVM中Heap的内存存储，其意思是*序列化*并cache到off-heap内存。

对于DISK和Memory两种级别是可以同时出现的。

关于OffHeap（毕竟是实验室里的作品）这里多说两句：JVM中如果直接进行内存分配都是受JVM来管理，使用的是JVM中内存堆，但是现在有很
多技术可以在JVM代码中访问不受JVM管理的内存，即OffHeap内存。OffHeap最大的好处就是将内存的管理工作从JVM的GC管理器剥离出来由自己
进行管理，特别是大对象，自定义生命周期的对象来说OffHeap很实用，可以减少GC的代销。具体实现是基于Spark的Off-heap实现的（说到底是
通过Java直接调JVM之外的内存，过去是基于Alluxio——前Tachyon——做的，在SPARK-12667中被移除了，该问题的描述见[SPARK-13992][4]）。

StorageLevel还提供了另外两个配置项：

* _deserialized：是否需要反序列化。
* _replication：副本数目。

存储在中BlockManager可以是各种对象，是否支持序列化影响了对这个对象的访问以及内存的压缩，`_deserialized`被标记为`true`之后，就不会
对数据进行序列化了。

***

对于BlockManager中每次`putBlockData`不一定都会成功，每次`getBlockData`不一定可以马上可以返回结果，因为put有等待的过程，而且可能最后还是失败。

BlockManager通过BlockInfo来维护每个Block的状态，在BlockManager中`blockInfoManager`（BlockInfoManager类型）来保存BlockId和BlockInfo的映射，
BlockInfoManager其实就是对映射表的又一层封装，只是增加了任务访问锁（写锁和读锁）来保证互斥写、同步读写（不要在读的时候写）。
下面来看一下`putBlockData`，其实际上最终调用的是`doPutBytes`

	private def doPutBytes[T](
		  blockId: BlockId,
		  bytes: ChunkedByteBuffer,
		  level: StorageLevel,
		  classTag: ClassTag[T],
		  tellMaster: Boolean = true,
		  keepReadLock: Boolean = false): Boolean = {
		doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
		  ...

		  val size = bytes.size

		  if (level.useMemory) {
			val putSucceeded = if (level.deserialized) {
			  val values =
				serializerManager.dataDeserializeStream(blockId, bytes.toInputStream())(classTag)
			  memoryStore.putIteratorAsValues(blockId, values, classTag) match {
				case Right(_) => true
				case Left(iter) =>
				  iter.close()
				  false
			  }
			} else {
			  memoryStore.putBytes(blockId, size, level.memoryMode, () => bytes)
			}
			if (!putSucceeded && level.useDisk) {
			  diskStore.putBytes(blockId, bytes)
			}
		  } else if (level.useDisk) {
			diskStore.putBytes(blockId, bytes)
		  }

		  val putBlockStatus = getCurrentBlockStatus(blockId, info)
		  val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
		  ...
		}.isEmpty
	}

	private def doPut[T](
		  blockId: BlockId,
		  level: StorageLevel,
		  classTag: ClassTag[_],
		  tellMaster: Boolean,
		  keepReadLock: Boolean)(putBody: BlockInfo => Option[T]): Option[T] = {

		val putBlockInfo = {
		  val newInfo = new BlockInfo(level, classTag, tellMaster)
		  if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {
			newInfo
		  } else {
			...
			return None
		  }
		}
		
		var blockWasSuccessfullyStored: Boolean = false
		val result: Option[T] = try {
		  val res = putBody(putBlockInfo)
		  blockWasSuccessfullyStored = res.isEmpty
		  res
		} finally {
		  ...
		}
		...
		result
	}
	
`doPutBytes`主要是依靠`doPut`来完成的，`doPutBytes`提供对BlockInfo的处理（包括Block的具体存储），`doPut`则用来
生成BlockInfo。`doPutBytes`中会先判断Block的存储层级，然后就是是否需要序列化，最后就是利用具体的BlockStore（`memoryStore`
或`diskStore`，[之后会介绍](##BlockStore)）的`putBytes`方法实现Block的存储。上面的代码省略了针对副本和注册等过程。

BlockInfo中的Block的状态是通过锁来控制的，即通过获取当前该Block的reader数量（读的时候写）或writer（写的时候读）来控制读写同步，
通过保证同时只有一个writer来保证互斥写。Block是否存储成功是通过`doPutBytes`中的`blockWasSuccessfullyStored`变量来控制的，
其是通过`getCurrentBlockStatus`来获取对应Id的Block的存储状态（就是该Block内存存了多少，磁盘存了多少，以及什么存储层级）来判断的。

##BlockStore

BlockStore即Block真正的存储器。但在Spark中，BlockStore既不是一个特质也不是一个类，这里只是用于统称。共分为MemoryStore和DiskStore
两个类型。

###MemoryStore

MemoryStore中有两张映射表：`onHeapUnrollMemoryMap`和`offHeapUnrollMemoryMap`。前者是从任务尝试的Id到*“Unroll”*一个Block所用内存的映射，
后者意思相同，不同的是前者是on-heap模式，后者的是off-heap模式。那么什么是“Unroll”？因为有的时候内存中的数据也会进行
序列化存储以减少对内存的消耗，但是最终还是要反序列化，由于序列化之后占用的的内存小，反序列化之后必然会膨胀，那么就要预留出一部分内存
来保证反序列化之后的数据有存储空间，所以数据反序列化之后所需的内存就是“Unroll”内存（[别人的理解，不过是Spark1.3][5]）。还有一种情况就是Block并不是一次性全部读入内存的，而是
以流的形式一部分一部分读入的，所以也需要一部分内存用于把Block中的数据“展开”（自己的理解）。怎么才能保证不会出现OOM呢？方法就是周期性地检查是否有足够
的空余内存，如果可以解压，就在“转换”到内存期间使用临时的“Unroll”内存。

下面来看怎么存储数据（代码太长，简而言之），在将数据存入memory时要检查是否有足够的内存，为什么序列化还要申请内存呢？我想应该是Block没有一次性
“Unroll”，一开始默认的给一部分内存用于Block的“Unroll”，“Unroll”过程中再不断申请内存。
顺着`MemoryStore.reserveUnrollMemoryForThisTask`->`UnifiedMemoryManager.acquireUnrollMemory`->`UnifiedMemoryManager.acquireUnrollMemory`看一下申请内存的函数。

	override def acquireStorageMemory(
		  blockId: BlockId,
		  numBytes: Long,
		  memoryMode: MemoryMode): Boolean = synchronized {
		assertInvariants()
		assert(numBytes >= 0)
		val (executionPool, storagePool, maxMemory) = memoryMode match {
		  case MemoryMode.ON_HEAP => (
			onHeapExecutionMemoryPool,
			onHeapStorageMemoryPool,
			maxOnHeapStorageMemory)
		  case MemoryMode.OFF_HEAP => (
			offHeapExecutionMemoryPool,
			offHeapStorageMemoryPool,
			maxOffHeapMemory)
		}
		if (numBytes > maxMemory) {
		  return false
		}
		if (numBytes > storagePool.memoryFree) {
		  val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree, numBytes)
		  executionPool.decrementPoolSize(memoryBorrowedFromExecution)
		  storagePool.incrementPoolSize(memoryBorrowedFromExecution)
		}
		storagePool.acquireMemory(blockId, numBytes)
	}
	
这里以UnifiedMemoryManager举例，因为其包含了off-heap和on-heap内存的申请，可以发现针对这二者有不同的内存池，当然还可以看到
运行内存，而且我们发现还有off-heap的运行内存（和用于存储的是两个概念，对off-heap内存的使用并不是指运行）。首先判断申请的
内存大小是否超过了上限，如果没超过继续看当前用于存储的内存池中的空间是否满足请求，如果不满足就将用于运行的内存池中的一部
分内存拿出来用于存储。进入StorageMemoryPool的`acquireMemory`，发现最后的腾出内存的操作是MemoryStore在`evictBlocksToFreeSpace`
中完成的，方法就是找到某个可以替换块（先利用写锁占有，保证没有reader和writer），删除掉其对应的信息。

在MemoryStore中的`putIteratorAsBytes`申请的off-heap内存是怎么实现的呢？

	//MemoryStore
	private[storage] def putIteratorAsBytes[T](
		  blockId: BlockId,
		  values: Iterator[T],
		  classTag: ClassTag[T],
		  memoryMode: MemoryMode): Either[PartiallySerializedBlock[T], Long] = {
		  ...
		val allocator = memoryMode match {
		  case MemoryMode.ON_HEAP => ByteBuffer.allocate _
		  case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
		}
		...
	}
	//Platform
	public static ByteBuffer allocateDirectBuffer(int size) {
		try {
		  Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
		  Constructor<?> constructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
		  constructor.setAccessible(true);
		  Field cleanerField = cls.getDeclaredField("cleaner");
		  cleanerField.setAccessible(true);
		  final long memory = allocateMemory(size);
		  ByteBuffer buffer = (ByteBuffer) constructor.newInstance(memory, size);
		  Cleaner cleaner = Cleaner.create(buffer, new Runnable() {
			@Override
			public void run() {
			  freeMemory(memory);
			}
		  });
		  cleanerField.set(buffer, cleaner);
		  return buffer;
		} catch (Exception e) {
		  throwException(e);
		}
		throw new IllegalStateException("unreachable");
	}

看到`allocateDirectBuffer`中的`cleaner`和`freeMemory`方法了吧。即使这里完成off-heap内存申请的。所以这里返
回的`allocator`已经是一个off-heap内存的ByteBuffer了。

###DiskStore

DiskStore即基于文件来存储Block。基于Disk来存储，首先必须要解决一个问题就是磁盘文件的管理：磁盘目录结构的组成，目录的清理等，
在Spark对磁盘文件的管理是通过DiskBlockManager来进行管理的，因此对DiskStore进行分析之前，首先必须对DiskBlockManager进行分析。

在Spark的配置信息中，通过"SPARK_LOCAL_DIRS"可以配置Spark运行过程中临时目录。有几点需要强调一下：

* `SPARK_LOCAL_DIRS`配置的是集合，即可以配置多个LocalDir，用","分开；这个和Hadoop中的临时目录等一样，可以在多个磁盘中创建localdir，从而分散磁盘的读写压力。
* spark运行过程中生成的子文件过程不可估计，这样很容易就会出现一个localDir中子文件过多，导致读写效率很差，针对这个问题，Spark在每个LocalDir中
创建了64个子目录，来分散文件。具体的子目录个数，可以通过`spark.diskStore.subDirectories`进行配置。

DiskBlockManager通过hash来分别确定localDir以及subdir。

	def getFile(filename: String): File = {
		// Figure out which local directory it hashes to, and which subdirectory in that
		val hash = Utils.nonNegativeHash(filename)
		val dirId = hash % localDirs.length
		val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

		// Create the subdirectory if it doesn't already exist
		val subDir = subDirs(dirId).synchronized {
		  val old = subDirs(dirId)(subDirId)
		  if (old != null) {
			old
		  } else {
			val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
			if (!newDir.exists() && !newDir.mkdir()) {
			  throw new IOException(s"Failed to create local dir in $newDir.")
			}
			subDirs(dirId)(subDirId) = newDir
			newDir
		  }
		}
		
		new File(subDir, filename)
	}
	
DiskBlockManager的核心工作就是这个，即提供`getFile`接口，根据filename确定一个文件的路径。剩下来的就是目录清理等工作。都比较简单这里就不进行详细分析。

至于DiskStore，就是将序列化后的数据（利用BlockManager进行序列化）写到DiskBlockManager获取到的文件位置中。

##BlockManager的服务结构

BlockManager分散在各个节点上，所以要有一个Master来收集各个节点的Block信息。这里依然利用RPC调用（每个节点保留一个对Master的引用，想其发送信息）。

###BlockManagerMaster服务

其是在SparkEnv中创建的，其功能通过函数名可以一目了然。主要作用有：

* 移除死Executor、某RDD的所有Block、某Shuffle的所有Block、特定Block和某广播变量的所有Block。
* 获取每个BlockManager Id所管理的内存状态或存储状态。
* 更新某BlockManager的中某Block的状态。
* 注册Blockmanager。
* 获取某Block的位置。

前面说BlockManager是对应于Executor的，那么是什么时候以及在哪里启动的呢？流程就是:

1. 任务分配到各节点；
2. Executor初始化；
3. BlockManager初始化中，先根据主机名、端口名和Executor Id生成BlockManagerId，然后调用BlockManagerMaster的`registerBlockManager`来注册本机的BlockManager；
4. `registerBlockManager`中通过RPC调用来向Master注册BlockManager。

###BlockManagerSlaveEndpoint

每个BlockManager中都有一个BlockManagerSlaveEndpoint类型的变量`slaveEndpoint`用于和Master通信，作用无非就是收到Master的消息进行相应处理，这里不再赘述。

###BlockTransferService

会发现在BlockManager中还有一个BlockTransferService类型的变量`blockTransferService`，其作用是从远程主机上把数据迁移过来。
主要的函数有

* `fetchBlocks`：从指定的远程的主机上将指定的Block拉过来。Shuffle过程会用到。
* `uploadBlock`：把本台主机上的一个Block传送到指定的远程主机。
* `fetchBlockSync`和`uploadBlockSync`与上面两个方法作用一样，但是会阻塞。

具体的文件拉取操作在OneForOneBlockFetcher的`start`方法（被NettyBlockTransferService的`fetchBlocks`调用）中。发送操作在NettyBlockTransferService的`uploadBlock`中。


[1]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/spark_shuffle.md
[2]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/spark_sort_shuffle.md
[3]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/memory_manager.md
[4]:https://github.com/apache/spark/pull/11805
[5]:https://0x0fff.com/spark-architecture/
