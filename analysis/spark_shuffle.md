#Spark基础及Shuffle实现

##Job，Stage，Task，Dependency

###从一般Job（非Shuffle）开始

Job可以看作是一组transformation操作和一个action操作的集合，换句话说每个action操作会触发一个Job。查看源码
可以发现每个action操作的最后总是调用`sc.runJob(this,...)`，即SparkContext下的runJob方法。其实所有的runJob的
重载方法最终都会调用如下的过程，

	def runJob[T, U: ClassTag](
		  rdd: RDD[T],
		  func: (TaskContext, Iterator[T]) => U,
		  partitions: Seq[Int],
		  resultHandler: (Int, U) => Unit): Unit = {
		if (stopped.get()) {
		  throw new IllegalStateException("SparkContext has been shutdown")
		}
		val callSite = getCallSite
		val cleanedFunc = clean(func)
		logInfo("Starting job: " + callSite.shortForm)
		if (conf.getBoolean("spark.logLineage", false)) {
		  logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
		}
		dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
		progressBar.foreach(_.finishAll())
		rdd.doCheckpoint()
	}
`resultHandler`参数是由之前的runJob传入的，主要作用是利用回调函数来返回结果，注意这里并不是利用方法返回值实现结果返回。
`func`参数是针对每个partition运行action操作的函数。`partitions`是各个partition编号构成的数组。dagScheduler返回Spark最外
层的调度器，通过名字也可以知道Job是一个DAG图，`dagScheduler.runJob`是一个堵塞操作，Job完成之后，rdd会进行checkpoint。
继续深入DAGScheduler.runJob

	def runJob[T, U](
		  rdd: RDD[T],
		  func: (TaskContext, Iterator[T]) => U,
		  partitions: Seq[Int],
		  callSite: CallSite,
		  resultHandler: (Int, U) => Unit,
		  properties: Properties): Unit = {
		val start = System.nanoTime
		val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
		val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
		waiter.completionFuture.ready(Duration.Inf)(awaitPermission)
		waiter.completionFuture.value.get match {
		  case scala.util.Success(_) =>
			logInfo("Job %d finished: %s, took %f s".format
			  (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
		  case scala.util.Failure(exception) =>
			logInfo("Job %d failed: %s, took %f s".format
			  (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
			// SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
			val callerStackTrace = Thread.currentThread().getStackTrace.tail
			exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
			throw exception
		}
	}
可以发现该方法是堵塞的，具体的就是`waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)`，即提交作业后其实
就已经堵塞了。

	//DAGScheduler
	def submitJob[T, U](
		  rdd: RDD[T],
		  func: (TaskContext, Iterator[T]) => U,
		  partitions: Seq[Int],
		  callSite: CallSite,
		  resultHandler: (Int, U) => Unit,
		  properties: Properties): JobWaiter[U] = {
		// Check to make sure we are not launching a task on a partition that does not exist.
		val maxPartitions = rdd.partitions.length
		partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
		  throw new IllegalArgumentException(
			"Attempting to access a non-existent partition: " + p + ". " +
			  "Total number of partitions: " + maxPartitions)
		}

		val jobId = nextJobId.getAndIncrement()
		if (partitions.size == 0) {
		  // Return immediately if the job is running 0 tasks
		  return new JobWaiter[U](this, jobId, 0, resultHandler)
		}

		assert(partitions.size > 0)
		val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
		val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
		eventProcessLoop.post(JobSubmitted(
		  jobId, rdd, func2, partitions.toArray, callSite, waiter,
		  SerializationUtils.clone(properties)))
		waiter
	}
前两句是判断rdd的partition和传入的partitions的数目一致才可以处理，之后就是这个Job创建一个JobId，如果该rdd没有
partition会直接判定为处理成功（直接返回一个JobWaiter对象），反之利用JobId和该rdd创建一个JobWaiter对象，然后向
`eventProcessLoop`，即DAG调度器，发送一个JobSubmitted的消息。由于现在的版本的Spark已经用Netty代替了Akka，所以
并不是Akka风格的写法。这里的JobWaiter也被一起发送了出去，`waiter`对象封装了很多信息，包括分区数和用于接收结果的
回调函数。`eventProcessLoop`是一个DAGSchedulerEventProcessLoop对象，其在接收到一个event后会调用DAGScheduler的
handleJobSubmitted方法。

	private[scheduler] def handleJobSubmitted(jobId: Int,
		  finalRDD: RDD[_],
		  func: (TaskContext, Iterator[_]) => _,
		  partitions: Array[Int],
		  callSite: CallSite,
		  listener: JobListener,
		  properties: Properties) {
		var finalStage: ResultStage = null
		try {
		  // New stage creation may throw an exception if, for example, jobs are run on a
		  // HadoopRDD whose underlying HDFS files have been deleted.
		  finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
		} catch {
		  case e: Exception =>
			logWarning("Creating new stage failed due to exception - job: " + jobId, e)
			listener.jobFailed(e)
			return
		}
		val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)

		submitStage(finalStage)
	}
这里删去部分没用的代码（关于log信息和web统计页面），该方法的主要工作就是根据rdd和和jobId生成finalStage，并且
调用`submitStage(finalStage)`将其提交运行。这里涉及到stage，每个Job只有一个finalStage，它是整个DAG最后一个Stage，
但是中间依据情况会有多个（或0个）ShuffleStage（严格说是ShuffleMapStage），ShuffleStage是涉及到宽依赖才会有的。
进入createResultStage

	private def createResultStage(
		  rdd: RDD[_],
		  func: (TaskContext, Iterator[_]) => _,
		  partitions: Array[Int],
		  jobId: Int,
		  callSite: CallSite): ResultStage = {
		val parents = getOrCreateParentStages(rdd, jobId)
		val id = nextStageId.getAndIncrement()
		val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
		stageIdToStage(id) = stage
		updateJobIdStageIdMaps(jobId, stage)
		stage
	}
	private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
		getShuffleDependencies(rdd).map { shuffleDep =>
		  getOrCreateShuffleMapStage(shuffleDep, firstJobId)
		}.toList
	}
	private[scheduler] def getShuffleDependencies(
		  rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
		val parents = new HashSet[ShuffleDependency[_, _, _]]
		val visited = new HashSet[RDD[_]]
		val waitingForVisit = new Stack[RDD[_]]
		waitingForVisit.push(rdd)
		while (waitingForVisit.nonEmpty) {
		  val toVisit = waitingForVisit.pop()
		  if (!visited(toVisit)) {
			visited += toVisit
			toVisit.dependencies.foreach {
			  case shuffleDep: ShuffleDependency[_, _, _] =>
				parents += shuffleDep
			  case dependency =>
				waitingForVisit.push(dependency.rdd)
			}
		  }
		}
		parents
	}
首先是调用`getOrCreateParentStages(rdd, jobId)`来生成父Stage，可以发现在`getOrCreateParentStages`中是父Stage是依据
shuffle依赖生成的。进入`getShuffleDependencies`，可以发现只有在ShuffleDependency，即宽依赖，的情况下才会将其作为父依赖，其他
情况（也就是窄依赖）只是追溯其父依赖（ShuffleDependency），而且这里只会追溯一层依赖，例如：A<--B<--C表示C依赖（Shuffle依赖）B，
B依赖A，这里只会解析到B<--C。`getOrCreateParentStages`中为每个Shuffle依赖创建一个ShuffleStage，也就是ShuffleMapStage。
ShuffleMapStage有两个比较重要的函数，`isAvailable`判断当前Stage是否运行完成，判断条件一目了然。`addOutputLoc`则主要是对
`_numAvailableOutputs`进行修改，`outputLocs`是利用partition编号作为下标，每个编号对应的可能MapStatus（因为一个partition可能运行多次），
MapStatus会在之后进行介绍，该函数主要是在`createShuffleMapStage`中调用，每有一个partition有shuffle输出，就会调用一次，当所有partition输出
都完成时，该stage也就完成了。

	def isAvailable: Boolean = _numAvailableOutputs == numPartitions
	def addOutputLoc(partition: Int, status: MapStatus): Unit = {
		val prevList = outputLocs(partition)
		outputLocs(partition) = status :: prevList
		if (prevList == Nil) {
		  _numAvailableOutputs += 1
		}
	}
接下来分析MapStatus，

	private[spark] sealed trait MapStatus {
	  def location: BlockManagerId

	  def getSizeForBlock(reduceId: Int): Long
	}
其只包含一个对象location，即这个任务运行的位置，也就是Map任务的输出位置。还有对reduceId对应的Block的预测大小，其实预测的重点目的是针对预测值为0
的Block不进行计算。对于ResultStage是否完成的判断由如下两部分完成

	//ResultStage
	def activeJob: Option[ActiveJob] = _activeJob

	private[spark] class ActiveJob(
		val jobId: Int,
		val finalStage: Stage,
		val callSite: CallSite,
		val listener: JobListener,
		val properties: Properties) {

	  /**
	   * Number of partitions we need to compute for this job. Note that result stages may not need
	   * to compute all partitions in their target RDD, for actions like first() and lookup().
	   */
	  val numPartitions = finalStage match {
		case r: ResultStage => r.partitions.length
		case m: ShuffleMapStage => m.rdd.partitions.length
	  }

	  /** Which partitions of the stage have finished */
	  val finished = Array.fill[Boolean](numPartitions)(false)

	  var numFinished = 0
	}
ResultStage中用activeJob获取该Job，然后Job中有一个finished变量表示各个partition是否完成，对于finish的修改是在
`DAGScheduler.handleTaskCompletion(event: CompletionEvent)`中完成的。

接下来继续`submitJob`分析

	private def submitStage(stage: Stage) {
		val jobId = activeJobForStage(stage)
		if (jobId.isDefined) {
		  logDebug("submitStage(" + stage + ")")
		  if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
			val missing = getMissingParentStages(stage).sortBy(_.id)
			logDebug("missing: " + missing)
			if (missing.isEmpty) {
			  logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
			  submitMissingTasks(stage, jobId.get)
			} else {
			  for (parent <- missing) {
				submitStage(parent)
			  }
			  waitingStages += stage
			}
		  }
		} else {
		  abortStage(stage, "No active job for stage " + stage.id, None)
		}
	}
	private def getMissingParentStages(stage: Stage): List[Stage] = {
		val missing = new HashSet[Stage]
		val visited = new HashSet[RDD[_]]
		// We are manually maintaining a stack here to prevent StackOverflowError
		// caused by recursively visiting
		val waitingForVisit = new Stack[RDD[_]]
		def visit(rdd: RDD[_]) {
		  if (!visited(rdd)) {
			visited += rdd
			val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
			if (rddHasUncachedPartitions) {
			  for (dep <- rdd.dependencies) {
				dep match {
				  case shufDep: ShuffleDependency[_, _, _] =>
					val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
					if (!mapStage.isAvailable) {
					  missing += mapStage
					}
				  case narrowDep: NarrowDependency[_] =>
					waitingForVisit.push(narrowDep.rdd)
				}
			  }
			}
		  }
		}
		waitingForVisit.push(stage.rdd)
		while (waitingForVisit.nonEmpty) {
		  visit(waitingForVisit.pop())
		}
		missing.toList
	}
首先判断该Stage是否是等待（父Stage没有完成）、运行、失败等状态。然后用`getMissingParentStages(stage)`获取
finalStage的所有未处理的父Stage，并且迭代提交父Stage，并且把该finalStage加入waitingStage，如果没有父Stage，
那么就利用`submitMissingTasks(stage, jobId.get)`提交该Stage。

`getCacheLocs(rdd)`是获取该Stage处理的RDD的所有Partition的cache位置，主要就是借助`cacheLocs`变量，其表示的
是RDD和其partirion的cache位置的映射，它是一个HashMap，key是RDD的id，val是一个数组，下标为partition的id，内
容是cache位置。如果有没有处理完的partition，那么判断该RDD的依赖对应的Stage是否完成，如果没有完成，就把没有完
成的父Stage加入`missing`。

接下来分析`submitMissingTasks`

	private def submitMissingTasks(stage: Stage, jobId: Int) {
		//step1
		stage.pendingPartitions.clear()

		val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

		val properties = jobIdToActiveJob(jobId).properties

		runningStages += stage
		//step 2
		val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
		  stage match {
			case s: ShuffleMapStage =>
			  partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
			case s: ResultStage =>
			  partitionsToCompute.map { id =>
				val p = s.partitions(id)
				(id, getPreferredLocs(stage.rdd, p))
			  }.toMap
		  }
		} catch {
		  ...
		}

		stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
		listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
		
		//step3
		
		var taskBinary: Broadcast[Array[Byte]] = null
		try {
		  // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
		  // For ResultTask, serialize and broadcast (rdd, func).
		  val taskBinaryBytes: Array[Byte] = stage match {
			case stage: ShuffleMapStage =>
			  JavaUtils.bufferToArray(
				closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
			case stage: ResultStage =>
			  JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
		  }

		  taskBinary = sc.broadcast(taskBinaryBytes)
		} catch {
		  ...
		}
		
		//step4
		val tasks: Seq[Task[_]] = try {
		  stage match {
			case stage: ShuffleMapStage =>
			  partitionsToCompute.map { id =>
				val locs = taskIdToLocations(id)
				val part = stage.rdd.partitions(id)
				new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
				  taskBinary, part, locs, stage.latestInfo.taskMetrics, properties)
			  }

			case stage: ResultStage =>
			  partitionsToCompute.map { id =>
				val p: Int = stage.partitions(id)
				val part = stage.rdd.partitions(p)
				val locs = taskIdToLocations(id)
				new ResultTask(stage.id, stage.latestInfo.attemptId,
				  taskBinary, part, locs, id, properties, stage.latestInfo.taskMetrics)
			  }
		  }
		} catch {
		  ...
		}
		
		//step5
		if (tasks.size > 0) {
		  stage.pendingPartitions ++= tasks.map(_.partitionId)
		  taskScheduler.submitTasks(new TaskSet(
			tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
		  stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
		} else {
		  // Because we posted SparkListenerStageSubmitted earlier, we should mark
		  // the stage as completed here in case there are no tasks to run
		  markStageAsFinished(stage, None)

		  submitWaitingChildStages(stage)
		}
	}
该过程大体分为5步。

第1步是指明该Stage所需计算的partition（因为可能重新计算，所以存在部分partition已经算完）。

然后第2步就是针对不同的Stage选择该任务执行的位置，接着就是请求运行（每运行一个Stage都会请求），并且向`listenerBus`发送
`SparkListenerStageSubmitted`消息，只有在接收到这个消息后才可以测试任务是否可序列化。

第3步是将任务运行所需的信息进行序列化并且发送给各个任务，针对不同的Stage发送的信息稍有不同，首先ShuffleStage和ResultStage都需要RDD信息，所以每个
任务都有一份RDD引用的拷贝，但是ShuffleStage需要额外发送依赖（ShuffleDependency），而ResultStage则必须有`func`信息。

	class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
		@transient private val _rdd: RDD[_ <: Product2[K, V]],
		val partitioner: Partitioner,
		val serializer: Serializer = SparkEnv.get.serializer,
		val keyOrdering: Option[Ordering[K]] = None,
		val aggregator: Option[Aggregator[K, V, C]] = None,
		val mapSideCombine: Boolean = false)

可以发现ShuffleDependency包含了partitioner告诉我们要按照什么分区函数将Map分Bucket进行输出, 有serializer告诉我们怎么
对Map的输出进行 序列化, 有keyOrdering和aggregator告诉我们怎么按照Key进行分Bucket,已经怎么进行合并,以及mapSideCombine
告诉我们是否需要进行Map端reduce。

第4步就是针对每个计算的分片构造Task对象，分别是`ShuffleMapTask`和`ResultTask`，二者都接受任务的运行位置、partition数据、
该Stage的信息、还有前面生成的广播信息等。进入具体的`ShuffleMapTask`类中，主要函数就是`runTask`，对广播对象进行反序列化，然后利用
具体ShuffleManager的具体ShuffleWriter对数据进行处理（这部分会在[Spark的shuffle机制分析][1]中分析）。
而`ResultTask`的`runTask`的处理相对简单，除了反序列化广播信息外，剩余操作就是调用`func`来计算该partition的结果。

第5步是将所有任务通过任务调度器（具体分析见[Task调度机制][2]）进行提交。如果该Stage的任务已经完毕，那么就将其标记为完成，
并且开始调度等待的子Stage。

现在假设任务已经运行完毕，开始分析如何将任务的运行结果传递给之前分析的`waiter`。

	private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
		val task = event.task
		val taskId = event.taskInfo.id
		val stageId = task.stageId
		val taskType = Utils.getFormattedClassName(task)
		  
		listenerBus.post(SparkListenerTaskEnd(
		   stageId, task.stageAttemptId, taskType, event.reason, event.taskInfo, taskMetrics))
		   
		val stage = stageIdToStage(task.stageId)
		event.reason match {
		  case Success =>
			stage.pendingPartitions -= task.partitionId
			task match {
			  case rt: ResultTask[_, _] =>
				// Cast to ResultStage here because it's part of the ResultTask
				// TODO Refactor this out to a function that accepts a ResultStage
				val resultStage = stage.asInstanceOf[ResultStage]
				resultStage.activeJob match {
				  case Some(job) =>
					if (!job.finished(rt.outputId)) {
					  updateAccumulators(event)
					  job.finished(rt.outputId) = true
					  job.numFinished += 1
					  // If the whole job has finished, remove it
					  if (job.numFinished == job.numPartitions) {
						markStageAsFinished(resultStage)
						cleanupStateForJobAndIndependentStages(job)
						listenerBus.post(
						  SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
					  }

					  // taskSucceeded runs some user code that might throw an exception. Make sure
					  // we are resilient against that.
					  try {
						job.listener.taskSucceeded(rt.outputId, event.result)
					  } catch {
						...
					  }
					}
				  case None =>
					...
				}
				...
			}
		}
	}
这里删去了很多逻辑，基本只剩成功处理ResultTask的情况。不过，这段代码里并没有waiter的信息，实际上这里的`job.listener`就是每个Job对应的Waiter，因为JobWaiter是JobListener的
子类，那waiter是怎么传给Job的呢？其实在之前的`handleJobSubmitted`函数分析中，就可以发现变成了listener，而且利用finalStage
和listener生成了对应的ActiveJob对象。

首先向由listenerBus（以后会另外介绍）发送任务完成的信息，针对ResultTask，那么说明该Job也完成了，所以将Job标记为完成。
向listenerBus发送Job完成的信息。最后`job.listener.taskSucceeded(rt.outputId, event.result)`将任务的结果返回给`listener`（即`waiter`）。
JobWaiter的`taskSucceeded`方法中调用`resultHandler`，即利用所说的回调函数完成结果的返回（如下面的代码所示）。

	override def taskSucceeded(index: Int, result: Any): Unit = {
		// resultHandler call must be synchronized in case resultHandler itself is not thread safe.
		synchronized {
		  resultHandler(index, result.asInstanceOf[T])
		}
		if (finishedTasks.incrementAndGet() == totalTasks) {
		  jobPromise.success(())
		}
	}

###Shuffle Job执行过程

Shuffle包含两个过程：Shuffle Map和Shuffle reduce，类似于MapReduce中的map和reduce。Shuffle Map就是ShuffleMapStage，
ShuffleMapTask将数据写到相应文件中，并把文件位置以MapOutput返回给DAGScheduler，并更新Stage信息。Reduce是利用不同类型的RDD
来实现的。

####Shuffle Map过程

首先介绍DAGScheduler中的`mapOutputTracker`（MapOutputTrackerMaster对象）。MapOutputTracker用于记录每个Stage map输出的位置（MapStatus对象），
相当于是存储元数据信息，由于Driver端和Executor端有不同的实现，所以分为MapOutputTrackerMaster和MapOutputTrackerWorker。Master端用于
注册ShuffleId和MapOutput信息，而Executor端用于拉取这些信息。

	private[spark] class MapOutputTrackerMaster(conf: SparkConf,
		broadcastManager: BroadcastManager, isLocal: Boolean)
	  extends MapOutputTracker(conf) {
	  def registerShuffle(shuffleId: Int, numMaps: Int) {
		if (mapStatuses.put(shuffleId, new Array[MapStatus](numMaps)).isDefined) {
		  throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
		}
		shuffleIdLocks.putIfAbsent(shuffleId, new Object())
	  }
	  def registerMapOutputs(shuffleId: Int, statuses: Array[MapStatus], changeEpoch: Boolean = false) {
		mapStatuses.put(shuffleId, Array[MapStatus]() ++ statuses)
		if (changeEpoch) {
		  incrementEpoch()
		}
	  }
	  def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {
		var statuses: Array[MapStatus] = null
		var retBytes: Array[Byte] = null
		var epochGotten: Long = -1
		
		shuffleIdLock.synchronized {
		  
		  val (bytes, bcast) = MapOutputTracker.serializeMapStatuses(statuses, broadcastManager,
			isLocal, minSizeForBroadcast)
		  // Add them into the table only if the epoch hasn't changed while we were working
		  epochLock.synchronized {
			if (epoch == epochGotten) {
			  cachedSerializedStatuses(shuffleId) = bytes
			  if (null != bcast) cachedSerializedBroadcast(shuffleId) = bcast
			} else {
			  logInfo("Epoch changed, not caching!")
			  removeBroadcast(bcast)
			}
		  }
		  bytes
		}
	  }
	}
Master的内容比较多，这里只取出3个比较重要的函数，`registerShuffle`和`registerMapOutputs`就是分别注册ShuffleId和对应的
MapOutput信息。`getSerializedMapOutputStatuses`是用于序列化MapOutput信息（这里删去了从cache了的信息中查找的过程）。
其中`registerShuffle`和`getSerializedMapOutputStatuses`是在创建ShuffleMapStage的时候调用的，即之前提到的
`createShuffleMapStage`函数。`registerMapOutputs`是在任务结束后调用的，即`handleTaskCompletion`函数中。

	private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
		...
		event.reason match {
		  case Success =>
			stage.pendingPartitions -= task.partitionId
			task match {
			  ...
			  case smt: ShuffleMapTask =>
				val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
				updateAccumulators(event)
				val status = event.result.asInstanceOf[MapStatus]
				val execId = status.location.executorId
				if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
				  ...
				} else {
				  shuffleStage.addOutputLoc(smt.partitionId, status)
				}

				if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
				  markStageAsFinished(shuffleStage)
				  
				  mapOutputTracker.registerMapOutputs(
					shuffleStage.shuffleDep.shuffleId,
					shuffleStage.outputLocInMapOutputTrackerFormat(),
					changeEpoch = true)

				  clearCacheLocs()

				  if (!shuffleStage.isAvailable) {
					submitStage(shuffleStage)
				  } else {
					// Mark any map-stage jobs waiting on this stage as finished
					if (shuffleStage.mapStageJobs.nonEmpty) {
					  val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
					  for (job <- shuffleStage.mapStageJobs) {
						markMapStageJobAsFinished(job, stats)
					  }
					}
					submitWaitingChildStages(shuffleStage)
				  }
				}
			}
		}
	}
RPC通信的工作是在MapOutputTrackerMasterEndpoint类中实现的，它提供了一个消息收发的处理方式，因为MapOutputTracker
对象本身就有一个MapOutputTrackerMasterEndpoint类型的成员变量`trackerEndpoint`，所以可以将其看作是一个访问入口。
该入口的设置是在SparkEnv中的create方法中注册完成的（由`createDriverEnv`和`createExecutorEnv`调用）。至于Executor是
如何RPC拉去数据的，就是通过MapOutputTracker中的`getStatuses(shuffleId: Int)`方法实现的，具体过程不再赘述。

#####Shuffle机制

Map按照什么规则输出数据是由ShuffleManager决定的。

	private[spark] class BaseShuffleHandle[K, V, C](
		shuffleId: Int,
		val numMaps: Int,
		val dependency: ShuffleDependency[K, V, C])
	  extends ShuffleHandle(shuffleId)
	  
	private[spark] trait ShuffleManager {
	  
	  def registerShuffle[K, V, C](
		  shuffleId: Int,
		  numMaps: Int,
		  dependency: ShuffleDependency[K, V, C]): ShuffleHandle
		  
	  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]
	  
	  def getReader[K, C](
		  handle: ShuffleHandle,
		  startPartition: Int,
		  endPartition: Int,
		  context: TaskContext): ShuffleReader[K, C]
		  
	  def unregisterShuffle(shuffleId: Int): Boolean
	  
	  def shuffleBlockResolver: ShuffleBlockResolver
	  
	  def stop(): Unit
	}
首先看上面的ShuffleHandle的实现, 它只是一个shuffleId, numMaps和ShuffleDep的封装; 再看ShuffleManager提供的接口。

+ registerShuffle/unregisterShuffle：提供了Shuffle的注册和注销的功能，和上面谈到的MapOutputTracker一致，然后
返回一个ShuffleHandle对象，来对shuffle进行封装。
+ getWriter：MapTask调用，用于输出数据。
+ getReader：Reduce过程进行调用，即ShuffleRDD调用，用于读取Map输出的内容。这里设置了起始位置，是因为每个Map输出的
文件其实只有一个，只是针对不同的Reduce输出的偏移量不同，这部分在[Spark的shuffle机制分析][1]讨论。

运行过程已经在上面的ShuffleMapTask的runTask中介绍过了。

####Shuffle Reduce实现

Reduce对应的应该是之前讲的ResultTask中runTask的内容，其中操作的RDD包括：CoGroupedRDD、CustomShuffledRDD、ShuffledRDD、
ShuffledRowRDD和SubtractedRDD，以ShuffleRDD为例，其compute方法不同与一般的非Shuffle RDD。

	override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
		val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
		SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
		  .read()
		  .asInstanceOf[Iterator[(K, C)]]
	}

[1]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/spark_sort_shuffle.md
[2]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/task_schedule.md