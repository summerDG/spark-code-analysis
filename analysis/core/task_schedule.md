# Spark的任务调度机制
## 从TaskScheduler开始
TaskScheduler的主要作用就是获得需要处理的任务集合，并将其发送到集群进行处理。并且还有汇报任务运行状态的作用。
所以其是在Master端。具体有以下4个作用：

* 接收来自Executor的心跳信息，使Master知道该Executer的BlockManager还“活着”。
* 对于失败的任务进行重试。
* 对于stragglers（拖后腿的任务）放到其他的节点执行。
* 向集群提交任务集，交给集群运行。

### TaskScheduler创建
对于调度器，都是在SparkContext中进行创建的。

    // SparkContext初始化块中
    val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode)
    _schedulerBackend = sched
    _taskScheduler = ts
`createTaskScheduler(this, master, deployMode)`是利用部署方式创建具体的TaskScheduler，这里我们发现有一个`sched`
变量，SchedulerBackend类型，该类型表示不同的后端调度，什么是后端调度，例如：Local，Standalone，Yarn、Mesos等。
对于外部的（与Standalone对应）的后端，其都继承自CoarseGrainedSchedulerBackend，命名表明这只是个粗粒度的后端，具体的
细粒度的实现在不同后端系统模块有实现。所有外部调度器和后端的生成是依赖ClusterManager来完成的，外部的ClusterManager
都继承自ExternalClusterManager。通过ExternalClusterManager的`createTaskScheduler`和`createSchedulerBackend`来完成
对TaskScheduler和还有SchedulerBackend的生成。具体可以以mesos为例，去mesos模块下去看实现。
至于TaskScheduler，其主要的实现就是TaskSchedulerImpl，Local模式、Standalone模式和Mesos模式都是用的这个实现。这里列举
部分组合方式：

* Local模式：TaskSchedulerImpl+LocalSchedulerBackend
* Spark Standalone集群模式：TaskSchedulerImpl+StandaloneSchedulerBackend
* Mesos集群模式：TaskSchedulerImpl+MesosFineGrainedSchedulerBackend或MesosCoarseGrainedSchedulerBackend
* Yarn集群模式：YarnClusterScheduler+YarnClusterSchedulerBackend

TaskScheduler和SchedulerBackend对象生成之后，会利用`scheduler.initialize(backend)`将二者配套。

这里有个问题没有处理，就是之前说过TaskScheduler中会接收心跳信息，那么通信过程是在哪里实现的呢？答案是HeartbeatReceiver中，
HeartbeatReceiver是一个SparkListener，并且是一个RPC通信的入口，在其`receiveAndReply`中会有

	case TaskSchedulerIsSet =>
      scheduler = sc.taskScheduler
      context.reply(true)

这句将其TaskScheduler进行初始化，那么心跳的接收以及处理就实现了。同时对于丢失Executor的处理也是在HeartbeatReceiver中完成的。

### TaskScheduler启动

TaskScheduler的启动也是在SparkContext初始化块中完成的。以TaskSchedulerImpl和StandaloneSchedulerBackend为例，分析器启动过程。
TaskScheduler的启动函数首先会启动`backend`，并启动一个`speculationScheduler`守护线程，用于检测当前Job的speculatable tasks。
speculatable tasks的应用场景是，如果一个任务执行时间过长，那么就将其判断为speculatable tasks（拖后腿了），然后将这个任务分配在
另外一台机器上，这时同一个任务会有两个执行，其中一台机器成功返回之后，会通知另外一台机器结束任务。

进入StandaloneSchedulerBackend的`start`函数，首先通过配置生成Dirver的rpc访问点。SchedulerBackend的作用如下：

* 向cluster manager请求启动新的Executor，或关闭特定的Executor。
* 关闭特定Executor上的特定任务。
* 收集Worker信息，比如任务的尝试次数，计算资源（就是CPU核数）等。

TaskScheduler则是通过收集的`backend`收集的资源信息针对任务作出合理的资源分配。在利用`backend`初始化的时候，其已经利用不同
的调度模式建立了对应的任务池（Pool），可以将其简单理解为一组遵循特定调度算法的任务。

### TaskScheduler提交任务集

由于TaskScheduler的函数很多，但最为重要的毕竟是提交任务给集群进行处理。

	override def submitTasks(taskSet: TaskSet) {
		val tasks = taskSet.tasks
		this.synchronized {
		  val manager = createTaskSetManager(taskSet, maxTaskFailures)
		  val stage = taskSet.stageId
		  val stageTaskSets =
			taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
		  stageTaskSets(taskSet.stageAttemptId) = manager
		  val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
			ts.taskSet != taskSet && !ts.isZombie
		  }
		  schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

		  ...
		backend.reviveOffers()
	}

首先注意到它会针对每个任务集（一个Stage的所有任务）生成对应的TaskSetManager，该类会跟踪每个任务，如果其失败会
进行重试，以及对于该任务集通过延迟调度来使其尽量保证是本地调度。该类的主要方法就是`resourceOffer`，其主要是判断
task的locality并最终调用`addRunningTask`来添加运行的任务，并调用DAGScheduler来启动任务，以下是源码：

	def resourceOffer(
		  execId: String,
		  host: String,
		  maxLocality: TaskLocality.TaskLocality)
		: Option[TaskDescription] =
	  {
		if (!isZombie) {
		  val curTime = clock.getTimeMillis()

		  var allowedLocality = maxLocality

		  dequeueTask(execId, host, allowedLocality) match {
			case Some((index, taskLocality, speculative)) =>
			  val task = tasks(index)
			  val taskId = sched.newTaskId()
			  copiesRunning(index) += 1
			  val attemptNum = taskAttempts(index).size
			  val info = new TaskInfo(taskId, index, attemptNum, curTime,
				execId, host, taskLocality, speculative)
			  taskInfos(taskId) = info
			  taskAttempts(index) = info :: taskAttempts(index)
			  
			  // Serialize and return the task
			  val startTime = clock.getTimeMillis()
			  val serializedTask: ByteBuffer = try {
				Task.serializeWithDependencies(task, sched.sc.addedFiles, sched.sc.addedJars, ser)
			  } catch {
				  ...
			  }
			  addRunningTask(taskId)

			  sched.dagScheduler.taskStarted(task, info)
			  return Some(new TaskDescription(taskId = taskId, attemptNumber = attemptNum, execId,
				taskName, index, serializedTask))
			case _ =>
		  }
		}
		None
	}

这里出现了TaskLocality，那它是如何定义的呢？其实是一个枚举类，共分为PROCESS_LOCAL，NODE_LOCAL，NO_PREF，
RACK_LOCAL，ANY，分别表示同个进程里，同个node(即机器)上，同机架，随意，很明显优先级已达到小。`dequeueTask`
是使任务从等待的任务队列中出列，返回任务的索引、locality（因为传入了Executor Id和节点Id，所以可以判断）和是
否是speculative任务。然后生成对应的任务信息，并把任务进行序列化。将该任务加入正在运行的任务集合中之后，通过
DAGScheduler来启动任务。但是这里的启动仅仅指的是Driver端认为其启动了。真正和Slave是如何通信的呢？其实利用的
就是`backend`，下面我们一步步分析。

`submitTasks`中创建TaskSetManager中之后会将其加入`schedulableBuilder`中，其实就是`schedulableBuilder`的`rootPool`
中。这样TaskSetManager就算提交了，那什么时候才会触发执行操作呢？

顺着`rootPool`->`TaskSchedulerImpl.resourceOffers`->`CoarseGrainedSchedulerBackend.launchTasks`->`CoarseGrainedSchedulerBackend.makeOffers`->`CoarseGrainedSchedulerBackend.receive`。

	//CoarseGrainedSchedulerBackend
	override def receive: PartialFunction[Any, Unit] = {
		  case StatusUpdate(executorId, taskId, state, data) =>
			scheduler.statusUpdate(taskId, state, data.value)
			if (TaskState.isFinished(state)) {
			  executorDataMap.get(executorId) match {
				case Some(executorInfo) =>
				  executorInfo.freeCores += scheduler.CPUS_PER_TASK
				  makeOffers(executorId)
				  ...
			  }
			}

		  case ReviveOffers =>
			makeOffers()
		...
	}
	
发现是在任务的状态更新之后或者收到`ReviveOffers`消息之后，这个消息是通过`reviveOffers`方法发送的，正好我们在`submitTasks`
的最后一句发现有`backend.reviveOffers()`，所以也就是这时触发了申请资源的操作。

进入`makeOffers`

	//CoarseGrainedSchedulerBackend
	private def makeOffers() {
		  // Filter out executors under killing
		  val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
		  val workOffers = activeExecutors.map { case (id, executorData) =>
			new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
		  }.toSeq
		  launchTasks(scheduler.resourceOffers(workOffers))
	}

首先将活跃的节点及上面的空闲核数等信息提取出来，申请资源，生成任务信息，传入`launchTasks`。先分析资源申请的过程。

	//TaskSchedulerImpl
	def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
		
		// Randomly shuffle offers to avoid always placing tasks on the same set of workers.
		val shuffledOffers = Random.shuffle(offers)
		// Build a list of tasks to assign to each worker.
		val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
		val availableCpus = shuffledOffers.map(o => o.cores).toArray
		val sortedTaskSets = rootPool.getSortedTaskSetQueue

		var launchedTask = false
		for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
		  do {
			launchedTask = resourceOfferSingleTaskSet(
				taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
		  } while (launchedTask)
		}

		if (tasks.size > 0) {
		  hasLaunchedTask = true
		}
		return tasks
	}
	
上面的代码只保留了核心代码，首先该方法会对可用的Worker打乱顺序，因为这可避免每次都把任务分配给数组前面的Worker，
然后生成对应的任务描述信息，利用`rootPool`中的调度方式对任务进行排序，然后进入`resourceOfferSingleTaskSet`针对
每个任务集申请资源。

	private def resourceOfferSingleTaskSet(
		  taskSet: TaskSetManager,
		  maxLocality: TaskLocality,
		  shuffledOffers: Seq[WorkerOffer],
		  availableCpus: Array[Int],
		  tasks: Seq[ArrayBuffer[TaskDescription]]) : Boolean = {
		var launchedTask = false
		for (i <- 0 until shuffledOffers.size) {
		  val execId = shuffledOffers(i).executorId
		  val host = shuffledOffers(i).host
		  if (availableCpus(i) >= CPUS_PER_TASK) {
			try {
			  for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
				tasks(i) += task
				...
			  }
			} catch {
			  ...
			}
		  }
		}
		if (!launchedTask) {
		  taskSet.abortIfCompletelyBlacklisted(executorIdToHost.keys)
		}
		return launchedTask
	}
	
该方法主要是调用上面提到的TaskSetManager的`resourceOffer`，所以会返回序列化后的任务信息。然后加入到`tasks`中，
这样TaskSchedulerImpl的`resourceOffers`也完成了。现在来看`launchTasks`的处理过程。

	//CoarseGrainedSchedulerBackend
	private def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
		  for (task <- tasks.flatten) {
			val serializedTask = ser.serialize(task)
			if (serializedTask.limit >= maxRpcMessageSize) {
			  ...
			}
			else {
			  val executorData = executorDataMap(task.executorId)
			  ...
			  executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
			}
		  }
	}

首先取出序列化任务的信息，然后取出分配给该任务的Executor，然后向该Executor发送启动任务的消息。该消息被Executor端的
CoarseGrainedExecutorBackend接收到

	//CoarseGrainedExecutorBackend
	override def receive: PartialFunction[Any, Unit] = {
		...
		case LaunchTask(data) =>
		  if (executor == null) {
			exitExecutor(1, "Received LaunchTask command but executor was null")
		  } else {
			val taskDesc = ser.deserialize[TaskDescription](data.value)
			executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
			  taskDesc.name, taskDesc.serializedTask)
		  }
		...
	}
	
收到启动任务的信息后，会先进行任务信息的反序列化，然后该`executor`启动任务，Executor有一组线程组成。

	//Executor
	def launchTask(
		  context: ExecutorBackend,
		  taskId: Long,
		  attemptNumber: Int,
		  taskName: String,
		  serializedTask: ByteBuffer): Unit = {
		val tr = new TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName,
		  serializedTask)
		runningTasks.put(taskId, tr)
		threadPool.execute(tr)
	}

该方法会从线程池中选择可用线程运行任务。

然后就是序列化任务结果，将其交给`ExecutorBackend`（调用CoarseGrainedExecutorBackend.statusUpdate），由`ExecutorBackend`
将结果返回给Driver端的SchedulerBackend（CoarseGrainedSchedulerBackend.receive），其收到会更新状态，即`statusUpdate`
方法。

	def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
	    var failedExecutor: Option[String] = None
	    synchronized {
	      try {
	        ...
	        taskIdToTaskSetManager.get(tid) match {
	          case Some(taskSet) =>
	            if (TaskState.isFinished(state)) {
	              ...
	            }
	            if (state == TaskState.FINISHED) {
	              taskSet.removeRunningTask(tid)
	              taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
	            } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
	              ...
	            }
	          case None =>
	            ...
	        }
		  }
		  ...
		}
		...
	}

这里仅保留任务成功结束的处理情况，可以发现首先是移除掉任务编号，然后取出任务结果。

	def enqueueSuccessfulTask(
		  taskSetManager: TaskSetManager,
		  tid: Long,
		  serializedData: ByteBuffer): Unit = {
		getTaskResultExecutor.execute(new Runnable {
		  override def run(): Unit = Utils.logUncaughtExceptions {
			try {
			  val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
				case directResult: DirectTaskResult[_] =>
				  if (!taskSetManager.canFetchMoreResults(serializedData.limit())) {
					return
				  }
				  
				  directResult.value()
				  (directResult, serializedData.limit())
				case IndirectTaskResult(blockId, size) =>
				  ...
			  }

			  ...

			  scheduler.handleSuccessfulTask(taskSetManager, tid, result)
			} catch {
			  ...
			}
		  }
		})
	}

取出任务结果后，交给TaskScheduler来处理成功的任务，其实是调用TaskSetManager的`handleSuccessfulTask`方法。该方法
调用DAGScheduler的`taskEnded`方法来发送CompletionEvent类型的消息（简而言之就是TaskSetManager向DAGScheduler发送消息），
DAGScheduler收到该消息后就会调用`handleTaskCompletion`方法进行处理（见[Spark基础及Shuffle实现][1]）。

至此任务的提交处理的整个过程就走通了。

[1]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/spark_shuffle.md
