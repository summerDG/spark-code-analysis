# Spark的shuffle机制分析
## SortShuffle
spark2.0.0开始后，HashShuffleManager被移除了，但实际上HashShuffle通过改变特定的ShuffleWriter来实现了，因
为这两个Shuffle的ShuffleReader是相同的，即reduce获取数据的方式相同。所以这里只分析SortShuffleManager。

SortShuffleManager的主要功能是依靠IndexShuffleBlockResolver、BlockStoreShuffleReader和ShuffleWriter的3个
子类（UnsafeShuffleWriter、bypassMergeSortShuffleWriter和BaseShuffleWriter）实现的。

在看下面的源码之前先看一下总体的思路，见[各种ShuffleWriter][1]。

### IndexShuffleBlockResolver分析
IndexShuffleBlockResolver用于生成并维护逻辑块到物理文件位置的映射。同一个map任务的shuffle块数据存储在一个
统一的数据文件中。数据块在数据文件中的偏移量存储在不同的索引文件中。数据文件的命名方式为shuffle_shuffleID_
mapId_reduceId.data，但是reduceId是被设置为0的，索引文件只是后缀为.index。

    private[spark] class IndexShuffleBlockResolver(
        conf: SparkConf,
        _blockManager: BlockManager = null)
      extends ShuffleBlockResolver
      with Logging {
      override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
        // The block is actually going to be a range of a single map output file for this map, so
        // find out the consolidated file, then the offset within that from our index
        val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)
        val in = new DataInputStream(new FileInputStream(indexFile))
        try {
          ByteStreams.skipFully(in, blockId.reduceId * 8)
          val offset = in.readLong()
          val nextOffset = in.readLong()
          new FileSegmentManagedBuffer(
            transportConf,
            getDataFile(blockId.shuffleId, blockId.mapId),
            offset,
            nextOffset - offset)
        } finally {
          in.close()
        }
      }
      def writeIndexFileAndCommit(
          shuffleId: Int,
          mapId: Int,
          lengths: Array[Long],
          dataTmp: File): Unit = {
        val indexFile = getIndexFile(shuffleId, mapId)
        val indexTmp = Utils.tempFileWith(indexFile)
        val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
        Utils.tryWithSafeFinally {
          // We take in lengths of each block, need to convert it to offsets.
          var offset = 0L
          out.writeLong(offset)
          for (length <- lengths) {
            offset += length
            out.writeLong(offset)
          }
        } {
          out.close()
        }
        val dataFile = getDataFile(shuffleId, mapId)
        // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
        // the following check and rename are atomic.
        synchronized {
          val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
          if (existingLengths != null) {
            // Another attempt for the same task has already written our map outputs successfully,
            // so just use the existing partition lengths and delete our temporary map outputs.
            System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
            if (dataTmp != null && dataTmp.exists()) {
              dataTmp.delete()
            }
            indexTmp.delete()
          } else {
            // This is the first successful attempt in writing the map outputs for this task,
            // so override any existing index and data files with the ones we wrote.
            if (indexFile.exists()) {
              indexFile.delete()
            }
            if (dataFile.exists()) {
              dataFile.delete()
            }
            if (!indexTmp.renameTo(indexFile)) {
              throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
            }
            if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
              throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
            }
          }
        }
    }

实际上，这里的reduceId由于一直保持0，所以并不是每个map和reduce都对应一个data和index文件。从上面的代码可以
看出获取索引文件靠的只是shuffleId和mapId，然后通过`ByteStreams.skipFully(in, blockId.reduceId * 8)`将in的
指针偏移量移动`blockId.reduceId * 8`，这样就可以找到该reduceId对应的块索引，通过获取前后两个块索引（偏移量），
就可以知道数据块的范围。writeIndexFileAndCommit会将每个块的偏移量写入索引文件，并且commit数据文件和索引文件，
`lengths`表示当前shuffle的数据块大小。我们可以看到每个map都只有一个data文件和index文件。这里在写数据文件的
时候首先检查索引文件与数据文件是否匹配，并且对于已经成功写入map输出文件情况，使用的是已有的分块长度，并且
要删除临时的map输出（包括数据文件和索引文件）。对于不匹配的情况（通常是每个task第一次尝试），要用临时的map输
出进行覆盖。

### ShuffleWriter分析
下面分析ShuffleWriter的逻辑。

    private[spark] class SortShuffleWriter[K, V, C](
        shuffleBlockResolver: IndexShuffleBlockResolver,
        handle: BaseShuffleHandle[K, V, C],
        mapId: Int,
        context: TaskContext)
      extends ShuffleWriter[K, V] with Logging {
      /** Write a bunch of records to this task's output */
      override def write(records: Iterator[Product2[K, V]]): Unit = {
        sorter = if (dep.mapSideCombine) {
          require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
          new ExternalSorter[K, V, C](
            context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
        } else {
          // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
          // care whether the keys get sorted in each partition; that will be done on the reduce side
          // if the operation being run is sortByKey.
          new ExternalSorter[K, V, V](
            context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
        }
        sorter.insertAll(records)
        // Don't bother including the time to open the merged output file in the shuffle write time,
        // because it just opens a single file, so is typically too fast to measure accurately
        // (see SPARK-3570).
        val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
        val tmp = Utils.tempFileWith(output)
        val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
        val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
        shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
        mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
      }
    }

首先分析默认的ShuffleWriter，SortShuffleWriter，其实现依赖于ExternalSorter，即外部排序，看源码的注释，
原理是基桶排序，先将key分到不同的partition中，然后每个partition中单独进行排序，并且这里有合并参数，
控制是否对相同key的value进行合并。经过排序后将不同partition的数据写到文件中。

    final class BypassMergeSortShuffleWriter<K, V> extends ShuffleWriter<K, V> {

      public void write(Iterator<Product2<K, V>> records) throws IOException {
        assert (partitionWriters == null);
        if (!records.hasNext()) {
          partitionLengths = new long[numPartitions];
          shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, null);
          mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
          return;
        }
        final SerializerInstance serInstance = serializer.newInstance();
        final long openStartTime = System.nanoTime();
        partitionWriters = new DiskBlockObjectWriter[numPartitions];
        partitionWriterSegments = new FileSegment[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
          final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
            blockManager.diskBlockManager().createTempShuffleBlock();
          final File file = tempShuffleBlockIdPlusFile._2();
          final BlockId blockId = tempShuffleBlockIdPlusFile._1();
          partitionWriters[i] =
            blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
        }
        // Creating the file to write to and creating a disk writer both involve interacting with
        // the disk, and can take a long time in aggregate when we open many files, so should be
        // included in the shuffle write time.
        writeMetrics.incWriteTime(System.nanoTime() - openStartTime);
        while (records.hasNext()) {
          final Product2<K, V> record = records.next();
          final K key = record._1();
          partitionWriters[partitioner.getPartition(key)].write(key, record._2());
        }
        
        for (int i = 0; i < numPartitions; i++) {
          final DiskBlockObjectWriter writer = partitionWriters[i];
          partitionWriterSegments[i] = writer.commitAndGet();
          writer.close();
        }
        
        File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
        File tmp = Utils.tempFileWith(output);
        partitionLengths = writePartitionedFile(tmp);
        shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
        mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
      }
    }

BypassMergeSortShuffleWriter就是过去的HashShuffle，从代码中可以看到`partitionWriters`会为每个map和
reduce生成一个partition文件writer，但是由于这会造成大量的小文件，所以这里会将这些小文件合并成一个大
文件，然后reduce在读取的时候同样是利用偏移量进行读取。`partitionWriterSegments`可以看作partition文件
的集合，然后调用`partitionLengths = writePartitionedFile(tmp)`将其彻底合并为一个文件，`partitionLengths`
对应的则是各个分片的大小，所以利用同样的原理生成索引文件。这种shuffle方式只有在特定的条件下会发挥不错，
具体可以看代码注释。


    public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

      public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
        // Keep track of success so we know if we encountered an exception
        // We do this rather than a standard try/catch/re-throw to handle
        // generic throwables.
        boolean success = false;
        try {
          while (records.hasNext()) {
            insertRecordIntoSorter(records.next());
          }
          closeAndWriteOutput();
          success = true;
        } finally {
          if (sorter != null) {
            try {
              sorter.cleanupResources();
            } catch (Exception e) {
              // Only throw this error if we won't be masking another
              // error.
              if (success) {
                throw e;
              } else {
                logger.error("In addition to a failure during writing, we failed during " +
                             "cleanup.", e);
              }
            }
          }
        }
      }
      @VisibleForTesting
      void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
        assert(sorter != null);
        final K key = record._1();
        final int partitionId = partitioner.getPartition(key);
        serBuffer.reset();
        serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
        serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
        serOutputStream.flush();
        
        final int serializedRecordSize = serBuffer.size();
        assert (serializedRecordSize > 0);
        
        sorter.insertRecord(
          serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
      }
      
      void closeAndWriteOutput() throws IOException {
        assert(sorter != null);
        updatePeakMemoryUsed();
        serBuffer = null;
        serOutputStream = null;
        final SpillInfo[] spills = sorter.closeAndGetSpills();
        sorter = null;
        final long[] partitionLengths;
        final File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
        final File tmp = Utils.tempFileWith(output);
        try {
          partitionLengths = mergeSpills(spills, tmp);
        } finally {
          for (SpillInfo spill : spills) {
            if (spill.file.exists() && ! spill.file.delete()) {
              logger.error("Error while deleting spill file {}", spill.file.getPath());
            }
          }
        }
        shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
        mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
      }
      
      private long[] mergeSpills(SpillInfo[] spills, File outputFile) throws IOException {
        final boolean compressionEnabled = sparkConf.getBoolean("spark.shuffle.compress", true);
        final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
        final boolean fastMergeEnabled =
          sparkConf.getBoolean("spark.shuffle.unsafe.fastMergeEnabled", true);
        final boolean fastMergeIsSupported = !compressionEnabled ||
          CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
        try {
          if (spills.length == 0) {
            new FileOutputStream(outputFile).close(); // Create an empty file
            return new long[partitioner.numPartitions()];
          } else if (spills.length == 1) {
            // Here, we don't need to perform any metrics updates because the bytes written to this
            // output file would have already been counted as shuffle bytes written.
            Files.move(spills[0].file, outputFile);
            return spills[0].partitionLengths;
          } else {
            final long[] partitionLengths;
            
            if (fastMergeEnabled && fastMergeIsSupported) {
              // Compression is disabled or we are using an IO compression codec that supports
              // decompression of concatenated compressed streams, so we can perform a fast spill merge
              // that doesn't need to interpret the spilled bytes.
              if (transferToEnabled) {
                logger.debug("Using transferTo-based fast merge");
                partitionLengths = mergeSpillsWithTransferTo(spills, outputFile);
              } else {
                logger.debug("Using fileStream-based fast merge");
                partitionLengths = mergeSpillsWithFileStream(spills, outputFile, null);
              }
            } else {
              logger.debug("Using slow merge");
              partitionLengths = mergeSpillsWithFileStream(spills, outputFile, compressionCodec);
            }
            
            writeMetrics.decBytesWritten(spills[spills.length - 1].file.length());
            writeMetrics.incBytesWritten(outputFile.length());
            return partitionLengths;
          }
        } catch (IOException e) {
          if (outputFile.exists() && !outputFile.delete()) {
            logger.error("Unable to delete output file {}", outputFile.getPath());
          }
          throw e;
        }
      }
    }

UnsafeShuffleWriter与SortShuffleWriter最大的不同在于二者使用的排序对象。前者使用的是ShuffleExternalSorter，
其排序使用的是ShuffleInMemorySorter，但该Sorter本身使用的排序有两种可供选择：RadixSort和TimSort。而且排序是
基于堆外记录的地址（不是Java对象），这会减少堆内内存开销和GC。
与ExternalSorter不同的是，其不会对溢出（内存装不下）数据进行合并，而是将合并工作转交给UnsafeShuffleWriter，
由于特殊的处理过程，可以省去额外的序列化和反序列化的操作。可以发现write的操作主要是调用insertRecordIntoSorter
插入数据，然后也是调用`sorter.insertRecord(...)`来完成，进一步探索就可以看到其只是在数据太大（内存装不下）的
时候将一部分数据放到磁盘上，减轻内存压力。然后在UnsafeShuffleWriter中调用mergeSpills完成对溢出数据的合并，基于
溢出文件数和IO压缩编解码来选择最快的合并策略。这需要序列化器允许序列化后的记录可以不用反序列化就可以连接合并。
此外对堆外内存数据的排序仅需要对其在堆内的地址映射进行排序，堆外的对象无需移动，所以ShuffleExternalSorter仅需要依据key对其地址（堆内的数组里）进行排序。有两种快速合并溢出文件的方式，FileStream和TransferTo。这两种方式直
接在序列化记录上操作，而不需要在合并的时候反序列化（其区别在于压缩方式是否支持之间合并）。而且如果压缩编解码支持压缩数据连接操作，那么就可以直接将文
件连接起来（但依据不同的压缩方式而定）。所以这一切都得益于编码和压缩，这种合并并不适应所有情况，如aggregation，
输出有序，因为而这会涉及到全局的数据信息，所以还需解压，这种情况下，就会退化成SortShuffleManager。

insertRecordIntoSorter中先是利用`serOutputStream`将record加入序列化流，但这里还有一个`serBuffer`是byte数组形式
的输出流，这二者的关系在open方法中有体现，`serBuffer`实际上是一个ByteArrayOutputStream的对象，只是它对外暴露了
自己的`buf`

    private void open() throws IOException {
        assert (sorter == null);
        sorter = new ShuffleExternalSorter(
          memoryManager,
          blockManager,
          taskContext,
          initialSortBufferSize,
          partitioner.numPartitions(),
          sparkConf,
          writeMetrics);
        serBuffer = new MyByteArrayOutputStream(1024 * 1024);
        serOutputStream = serializer.serializeStream(serBuffer);
    }

所以在

    sorter.insertRecord(
          serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);

这句中可以看到，利用`serBuffer`直接将buffer中的内容传到了sorter中，由其进行排序。然后在closeAndWriteOutput方法中
对数据进行写文件的操作，前面的部分类似于其他Writer，不同的是`partitionLengths = mergeSpills(spills, tmp);`，而spills
则是sorter的输出，spills的类型是`SpillInfo[]`，这个类其实就是记录了溢出数据的文件，以及包含的分片长度数组和块id。
转入mergeSpills，可以发现压缩编解码的内容，但是并没有发现和sorter或者spills的联系，其实压缩编解码的过程是在`SerializerManager`
中完成的，其实也就是序列化的过程，该类的作用就是依据配置自动选择序列化方式和压缩方式。然后就是依据不同的压缩配置
来选择不同的合并方式，`mergeSpillsWithTransferTo`是基于NIO转换的合并方式，速度快，但当压缩和序列化支持连接操作时。
`mergeSpillsWithFileStream`更适用压缩不支持直接合并的情况，因为该过程需要先解压缩，然后再进行压缩，所以速度相对慢一点。

### ShuffleReader分析
SortShuffleManager依赖的ShuffleReader是BlockStoreShuffleReader，其作用就是从其他节点将该reduce所要读取的数据段拉过来。
其只包含方法read

    private[spark] class BlockStoreShuffleReader[K, C](
        handle: BaseShuffleHandle[K, _, C],
        startPartition: Int,
        endPartition: Int,
        context: TaskContext,
        serializerManager: SerializerManager = SparkEnv.get.serializerManager,
        blockManager: BlockManager = SparkEnv.get.blockManager,
        mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
      extends ShuffleReader[K, C] with Logging {
      
      override def read(): Iterator[Product2[K, C]] = {
      
        val blockFetcherItr = new ShuffleBlockFetcherIterator(
          context,
          blockManager.shuffleClient,
          blockManager,
          mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
          SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
          SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue))
        
        // Wrap the streams for compression based on configuration
        val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
          serializerManager.wrapForCompression(blockId, inputStream)
        }
        
        val serializerInstance = dep.serializer.newInstance()
        
        // Create a key/value iterator for each stream
        val recordIter = wrappedStreams.flatMap { wrappedStream =>
          serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
        }
        
        // An interruptible iterator must be used here in order to support task cancellation
        val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)
        
        val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
          if (dep.mapSideCombine) {
            // We are reading values that are already combined
            val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
            dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
          } else {
            // We don't know the value type, but also don't care -- the dependency *should*
            // have made sure its compatible w/ this aggregator, which will convert the value
            // type to the combined type C
            val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
            dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
          }
        } else {
          require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
          interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
        }
        
        // Sort the output if there is a sort ordering defined.
        dep.keyOrdering match {
          case Some(keyOrd: Ordering[K]) =>
            // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
            // the ExternalSorter won't spill to disk.
            val sorter =
              new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
            sorter.insertAll(aggregatedIter)
            context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
            context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
            context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
            CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
          case None =>
            aggregatedIter
        }
      }
    }

为了简洁，这里删去部分代码，前两句是获取打包后的数据（因为不同节点对应的map输出并不在一起），第一句使用到
`mapOutputTracker`，也就是reduce必须通过它才能知道自己应该拉取的数据在哪台物理节点上。第4句是用于反序列化
数据。后面的内容是针对aggregate操作和输出排序的。


[1]: ./spark_shuffle_new.md
