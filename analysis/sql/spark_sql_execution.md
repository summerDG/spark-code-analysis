#Spark SQL 执行阶段

本文是从生成PhysicalPlan之后开始说起。

	//QueryExecution
	lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

	lazy val toRdd: RDD[InternalRow] = executedPlan.execute()
	
主要针对以上两条语句。首先`executedPlan`是对已生成的PhysicalPlan做一些处理，主要是插入shuffle操作和internal row的格式转换。

	  //QueryExecution
	  protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
		preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
	  }

	  protected def preparations: Seq[Rule[SparkPlan]] = Seq(
		python.ExtractPythonUDFs,
		PlanSubqueries(sparkSession),
		EnsureRequirements(sparkSession.sessionState.conf),
		CollapseCodegenStages(sparkSession.sessionState.conf),
		ReuseExchange(sparkSession.sessionState.conf),
		ReuseSubquery(sparkSession.sessionState.conf))

##Preparations规则

和Python相关的不介绍。

###PlanSubqueries

	case class PlanSubqueries(sparkSession: SparkSession) extends Rule[SparkPlan] {
	  def apply(plan: SparkPlan): SparkPlan = {
		plan.transformAllExpressions {
		  case subquery: expressions.ScalarSubquery =>
			val executedPlan = new QueryExecution(sparkSession, subquery.plan).executedPlan
			ScalarSubquery(
			  SubqueryExec(s"subquery${subquery.exprId.id}", executedPlan),
			  subquery.exprId)
		  case expressions.PredicateSubquery(query, Seq(e: Expression), _, exprId) =>
			val executedPlan = new QueryExecution(sparkSession, query).executedPlan
			InSubquery(e, SubqueryExec(s"subquery${exprId.id}", executedPlan), exprId)
		}
	  }
	}
	
1. 将ScalarSubquery生成SparkPlan，其实回顾上一篇[Spark SQL Physical Plan][1]，并没有对ScalarSubquery做处理。原因何在？实际上如果解析的是SQL语句，是不会产生这个节点的，
只有在人为产生LogicalPlan的时候才可能会出现。所以Subquery之前是一直没有被转化的，这里就进行转化，其对应的SparkPlan节点是ScalarSubquery（execution），和之前的ScalarSubquery（expressions）是不同的，二者属于不同的包，只是同名。
2. 将PredicateSubquery生成SparkPlan。何谓PredicateSubquery？该种子查询会检查其子查询结果中是否存在某个值，现在只允许将谓词表达式放在Filter Plan中（WHERE或HAVING块中）。实际上该查询和ScalarSubquery一样，如果直接解析SQL语句，是不会出现这个类的。
其实Subquery类的子类中只有这两类是SQL语句不会生成的，其他两个是Exist和ListQuery（即IN包含的块）。该规则最后生成InSubquery，该类就是检查查询中的记录包不包含谓词表达式的结果，所以该节点的结果有true、false、null三种。

###EnsureRequirements

	//EnsureRequirements
	def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
		case operator @ ShuffleExchange(partitioning, child, _) =>
		  child.children match {
			case ShuffleExchange(childPartitioning, baseChild, _)::Nil =>
			  if (childPartitioning.guarantees(partitioning)) child else operator
			case _ => operator
		  }
		case operator: SparkPlan => ensureDistributionAndOrdering(operator)
	}
	
1. 将ShuffleExchange类型的节点做处理，这个类只会出现在repartition的时候，所以如果该节点的子节点已经可以保证partition，那么就用子节点代替该节点。

	//EnsureRequirements
	private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
		val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
		val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
		assert(requiredChildDistributions.length == operator.children.length)
		assert(requiredChildOrderings.length == operator.children.length)

		def createShuffleExchange(dist: Distribution, child: SparkPlan) =
		  ShuffleExchange(createPartitioning(dist, defaultNumPreShufflePartitions), child)

		var (parent, children) = operator match {
		  case PartialAggregate(childDist) if !operator.outputPartitioning.satisfies(childDist) =>
			val (mergeAgg, mapSideAgg) = AggUtils.createMapMergeAggregatePair(operator)
			(mergeAgg, createShuffleExchange(requiredChildDistributions.head, mapSideAgg) :: Nil)
		  case _ =>
			// Ensure that the operator's children satisfy their output distribution requirements:
			val childrenWithDist = operator.children.zip(requiredChildDistributions)
			val newChildren = childrenWithDist.map {
			  case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
				child
			  case (child, BroadcastDistribution(mode)) =>
				BroadcastExchangeExec(mode, child)
			  case (child, distribution) =>
				createShuffleExchange(distribution, child)
			}
			(operator, newChildren)
		}

		def requireCompatiblePartitioning(distribution: Distribution): Boolean = distribution match {
		  case UnspecifiedDistribution => false
		  case BroadcastDistribution(_) => false
		  case _ => true
		}
		...

		children = withExchangeCoordinator(children, requiredChildDistributions)

		...

		parent.withNewChildren(children)
	}
	
2. 其他类型的节点的处理函数如上。

* DIstribution表示数据的分布方式，其分布方式有两个方面，集群分布和单个partition内的分布，分别表示数据如何被分布到集群上和每个partition内部的分布情况。`requiredChildDistribution`会有不同的实现，简单看了一下Aggregation操作的实现，其实就是尽量将有联系的tuple放在一起（用表达式判断联系）。作用单位是一个SparkPlan节点。
* SortOrder，一个表达式可以用于对tuple进行排序，作用单位是Expression。

可以发现`requiredChildDistributions`和`requiredChildOrderings`长度都是该SparkPlan的子节点个数，也就是每个子节点可能有不同的分布和排序。

然后利用PartialAggregate提取出该Plan第一个子节点的分布方式（同时判断是否支持partial aggregations，该操作类似于combiner，但适用范围更广，不支持该特性的操作不多，在[上一篇][1]中有介绍），并且其输出不满足该分布，那么就为其生成一个map端的aggregation节点和一个合并节点，并且利用map端的aggregation节点生成ShuffleExchange节点。
**简而言之，如果一个Aggregation操作需要Shuffle并且支持partial aggregations，先在map端进行部分aggregations，然后shuffle，最后合并。**

对于其他节点，就是如果子节点的输出已经满足该子节点对应的分布情况，就直接用子节点代替（省去了重新分布，即shuffle的过程）。如果子节点的分布模式是`BroadcastDistribution`，就为其生成一个`BroadcastExchangeExec`类型的节点。其他情况就直接生成ShuffleExchange节点。

省略的代码很长，主要功能就是当该SparkPlan有多个子节点的时候，并且指定了子节点的分布，那么子节点的输出分区一定要相互兼容。兼不兼容的判断条件是，分区数不同一定不兼容，分区数相同的情况下分区看策略是否相同。
如果不兼容，首先判断子节点的输出分区是否都满足各自的分布策略，如果是就不做处理（因为`requiredChildDistributions`中的分布是兼容的，因此这种情况下子节点输出分区也相互兼容）。
如果已不能用现有分区，先确定新的分区数，策略是如果所有子节点输出的分区与对应分布策略都不兼容，那么就用默认分区数目，反之则用这些节点中最大的分区数目。最后扫描各节点，如果与新的分布策略不匹配，那么就对其输出重新Shuffle（分布策略依然用对应分区策略，分区数目就是新确定的），形成新的分区。

接下来的工作就是为子节点增加ExchangeCoordinator，该类表示一个协调器，现在是想的作用就是确定Shuffle后partition的数目。

下面省略的代码表示为子节点增加必要的排序方式，策略类似于前面，就是如果子节点输出的排序已经满足对应的排序，即直接用子节点，否则就在其包裹一层排序节点。最后一句就是合并。其整体结构如下：

![add shuffle and sort][ensureDistributionAndOrdering]

###CollapseCodegenStages

该规则是在支持codegen的节点顶端插入WholeStageCodegen。可参考[Apache Spark as a Compiler: Joining a Billion Rows per Second on a Laptop][2]。

	//WholeStageCodegenExec.scala
	private def insertWholeStageCodegen(plan: SparkPlan): SparkPlan = plan match {
		// For operators that will output domain object, do not insert WholeStageCodegen for it as
		// domain object can not be written into unsafe row.
		case plan if plan.output.length == 1 && plan.output.head.dataType.isInstanceOf[ObjectType] =>
		  plan.withNewChildren(plan.children.map(insertWholeStageCodegen))
		case plan: CodegenSupport if supportCodegen(plan) =>
		  WholeStageCodegenExec(insertInputAdapter(plan))
		case other =>
		  other.withNewChildren(other.children.map(insertWholeStageCodegen))
	}
	private def supportCodegen(plan: SparkPlan): Boolean = plan match {
		case plan: CodegenSupport if plan.supportCodegen =>
		  val willFallback = plan.expressions.exists(_.find(e => !supportCodegen(e)).isDefined)
		  val hasTooManyOutputFields =
			numOfNestedFields(plan.schema) > conf.wholeStageMaxNumFields
		  val hasTooManyInputFields =
			plan.children.map(p => numOfNestedFields(p.schema)).exists(_ > conf.wholeStageMaxNumFields)
		  !willFallback && !hasTooManyOutputFields && !hasTooManyInputFields
		case _ => false
	}

只有一种条件（第二个case）会插入WholeStageCodegen。第一种条件表明输出类型是特定类型并且仅输出一个属性。那么什么算是支持codegen呢？首先必须继承了CodegenSupport特质，并且`supportCodegen`为true，
很多节点都实现了该trait，但是是否支持的判断条件各不相同，这里不赘述。这里有对`supportCodegen`的重载，`willFallBack`用于判断是否有表达式的代码生成支持回滚，为什么回滚的不能生成WholeStageCodegen？
大概是因为实现了该trait的类本身没有相应的执行方式，其操作太复杂或者有些第三方组件无法集成到生成的代码中，所以没有用java（scala）代码生成。`hasTooManyOutputFields`保证输出的数据类型不能有太多的field。
而且`hasTooManyInputFields`表明每个子节点的输入数据也都不能超出上限（conf.wholeStageMaxNumFields）。

###ReuseExchange

该规则是针对重复的Exchange的。Exchange是一个抽象类，所有子类都和多线程或进程的数据交换有关，之前提到的ShuffleExchange和BroadcastExchangeExec是它仅有的两个子类。如果之前有相同类型的Exchange，并且输出结果相同（条件就是判断递归输出类型，参数个数，子树大小是否相同，基本就是说树结构相同，输出结果就相同），
那么就用之前生成的节点替换，即重用之前生成的节点。该规则逻辑比较简单清晰，这里就不贴代码了。

###ReuseSubquery

处理基本同ReuseExchange，只是操作对象换成了ExecSubqueryExpression。

##execute方法介绍

每个SparkPlan都有一个`execute`方法，其调用`doExecute`方法，各个子类会实现具体的doExecute方法。这里以select XXX from XXX where XXX为例介绍。该语句涉及到的SparkPlan节点很少，只有FilterExec，ProjectExec和InMemoryTableScanExec（假设是InMemoryRealtion）。

![execution example][execution]

###ProjectExec执行

	//ProjectExec
	protected override def doExecute(): RDD[InternalRow] = {
		child.execute().mapPartitionsInternal { iter =>
		  val project = UnsafeProjection.create(projectList, child.output,
			subexpressionEliminationEnabled)
		  iter.map(project)
		}
	}

执行过程很简单就是针对子节点的输出RDD[InternalRow]，以partition为单位执行投影操作，注意这里生成的并不是最后的数据，而是RDD[InternalRow]，我们之前说明InternalRow并不保存实际数据。也就是execute是transform操作，而非action操作。
但是RDD[InternalRow]有一个很重要的作用就是知道每步操作的对象。例如：Row[InternalRow]记录了这一步是对第x个单元的数据加一，x是一个变量，但是最后执行的时候，这部分代码只是作用于真是的数据。简而言之RDD[InternalRow]用于生成相应代码。
上面函数中的`project`就相当于一个投影操作的代码生成函数。

###FilterExec执行

	//FilterExec
	protected override def doExecute(): RDD[InternalRow] = {
		val numOutputRows = longMetric("numOutputRows")
		child.execute().mapPartitionsInternal { iter =>
		  val predicate = newPredicate(condition, child.output)
		  iter.filter { row =>
			val r = predicate(row)
			if (r) numOutputRows += 1
			r
		  }
		}
	}
	
`predicate`相当于一个函数用于判断该行满不满足条件，不过和上面一样是代码生成的函数。

###InMemoryTableScanExec执行

该节点的`doExecute`方法比较复杂。但思路就是操作具体列（因为经过前面的优化后，没必要输出所有列），作为输出。

##与具体数据的联系

以`collect`操作为例。

	//DataSet
	def executeCollect(): Array[InternalRow] = {
		val byteArrayRdd = getByteArrayRdd()

		val results = ArrayBuffer[InternalRow]()
		byteArrayRdd.collect().foreach { bytes =>
		  decodeUnsafeRows(bytes).foreach(results.+=)
		}
		results.toArray
	}
	
	private def getByteArrayRdd(n: Int = -1): RDD[Array[Byte]] = {
		execute().mapPartitionsInternal { iter =>
		  var count = 0
		  val buffer = new Array[Byte](4 << 10)  // 4K
		  val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
		  val bos = new ByteArrayOutputStream()
		  val out = new DataOutputStream(codec.compressedOutputStream(bos))
		  while (iter.hasNext && (n < 0 || count < n)) {
			val row = iter.next().asInstanceOf[UnsafeRow]
			out.writeInt(row.getSizeInBytes)
			row.writeToStream(out, buffer)
			count += 1
		  }
		  out.writeInt(-1)
		  out.flush()
		  out.close()
		  Iterator(bos.toByteArray)
		}
	}
	
`byteArrayRdd`就是该DataSet对应的RDD[Array[Byte]]。`getByteArrayRdd`就是利用`execute`生成的RDD[InternalRow]。RDD[InternalRow]和数据联系的桥梁是两个：Interator[InternalRow]和UnsafeRow。

###Iterator[InternalRow]和UnsafeRow

InternalRow虽然不包含数据，但是并不妨碍Iterator[InternalRow]可以输出数据，因为`hasNext`，`next`等函数可以被重写。
上面在[InMemoryTableScanExec][###InMemoryTableScanExec执行]中没有提到的是其`execute`调用了`GenerateColumnAccessor.generate(columnTypes)`，而该函数又调用了`GenerateColumnAccessor.create(columnTypes)`。
`create`的主要功能就是生成代码（由于是字符串，所以IDE根本检测不到）。生成的代码主要是实现了SpecificColumnarIterator类，该类是ColumnarIterator（继承自Interator[InternalRow]）的子类。其中就有`buffers`，`rowWriter`等变量。

	//GenerateColumnAccessor
	protected def create(columnTypes: Seq[DataType]): ColumnarIterator = {
		...
		val codeBody = s"""
		  import ...

		  public SpecificColumnarIterator generate(Object[] references) {
			return new SpecificColumnarIterator();
		  }

		  class SpecificColumnarIterator extends ${classOf[ColumnarIterator].getName} {

			private ByteOrder nativeOrder = null;
			private byte[][] buffers = null;
			private UnsafeRow unsafeRow = new UnsafeRow($numFields);
			private BufferHolder bufferHolder = new BufferHolder(unsafeRow);
			private UnsafeRowWriter rowWriter = new UnsafeRowWriter(bufferHolder, $numFields);
			private MutableUnsafeRow mutableRow = null;

			private int currentRow = 0;
			private int numRowsInBatch = 0;

			private scala.collection.Iterator input = null;
			private DataType[] columnTypes = null;
			private int[] columnIndexes = null;

			...

			public void initialize(Iterator input, DataType[] columnTypes, int[] columnIndexes) {
			  this.input = input;
			  this.columnTypes = columnTypes;
			  this.columnIndexes = columnIndexes;
			}

			${ctx.declareAddedFunctions()}

			public boolean hasNext() {
			  ...
			}

			public InternalRow next() {
			  currentRow += 1;
			  bufferHolder.reset();
			  rowWriter.zeroOutNullBytes();
			  ${extractorCalls}
			  unsafeRow.setTotalSize(bufferHolder.totalSize());
			  return unsafeRow;
			}
		  }"""

		val code = CodeFormatter.stripOverlappingComments(
		  new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
		...
		CodeGenerator.compile(code).generate(Array.empty).asInstanceOf[ColumnarIterator]
	}

在这里就会体会到其实在Interator[InternalRow]就会有数据了。这里主要利用到4个变量：`buffer`，`unsafeRow`，`bufferHolder`，`rowWriter`。`buffer`的类型好解释。下面解释其他3个对象的类型。

####UnsafeRow

该类型继承自InternalRow，但其包含了数据，而且其操作是原生内存操作，所以不是Java对象。其数据保存在`baseObject`对象中，该对象是Object类型。

	//UnsafeRow
	public byte getByte(int ordinal) {
		assertIndexIsValid(ordinal);
		return Platform.getByte(baseObject, getFieldOffset(ordinal));
	}
	private long getFieldOffset(int ordinal) {
		return baseOffset + bitSetWidthInBytes + ordinal * 8L;
	}

每个Tuple由三部分组成：[null bit set] [values] [variable length portion]。`baseOffset`是一个对象的头部占用的长度。

* [null bit set] 用于null位的跟踪，长度是**8-Byte**（如果没有field，该域长度为0），其会为每个field存储一个**bit**，用0或1表示是否为null。
* [values]域中为每个field存储了8-Byte的内容，当然对于固定长度的原始类型，如long，int，double等，直接存入。对于非原始类型或可变长度的值，存储的是一个相对offset（指向变长field的起始位置）和该field的长度（[variable length portion]）。
所以UnsafeRow对象可以当做是一个指向原始数据的指针。

####BufferHolder

该类实际上是用于生成相应UnsafeRow的。

	//BufferHolder
	public BufferHolder(UnsafeRow row, int initialSize) {
		int bitsetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(row.numFields());
		if (row.numFields() > (Integer.MAX_VALUE - initialSize - bitsetWidthInBytes) / 8) {
		  ...
		}
		this.fixedSize = bitsetWidthInBytes + 8 * row.numFields();
		this.buffer = new byte[fixedSize + initialSize];
		this.row = row;
		this.row.pointTo(buffer, buffer.length);
	}
	//UnsafeRow
	public void pointTo(byte[] buf, int sizeInBytes) {
		pointTo(buf, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
	}
	public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
		assert numFields >= 0 : "numFields (" + numFields + ") should >= 0";
		this.baseObject = baseObject;
		this.baseOffset = baseOffset;
		this.sizeInBytes = sizeInBytes;
	}
	
以上3个方法就是将byte数组存入UnsafeRow中，真正写入的操作是由UnsafeRowWriter启动的。

####UnsafeRowWriter

针对不同的类型会有不同的写入方法，为了简洁，这里选用原始类型，以说明原理。

	//UnsafeRowWriter
	public void write(int ordinal, long value) {
		Platform.putLong(holder.buffer, getFieldOffset(ordinal), value);
	}
	public long getFieldOffset(int ordinal) {
		return startingOffset + nullBitsSize + 8 * ordinal;
	}
	public void reset() {
		this.startingOffset = holder.cursor;

		holder.grow(fixedSize);
		holder.cursor += fixedSize;

		zeroOutNullBytes();
	}
	//BufferHolder
	public void grow(int neededSize) {
		if (neededSize > Integer.MAX_VALUE - totalSize()) {
		  ...
		}
		final int length = totalSize() + neededSize;
		if (buffer.length < length) {
		  // This will not happen frequently, because the buffer is re-used.
		  int newLength = length < Integer.MAX_VALUE / 2 ? length * 2 : Integer.MAX_VALUE;
		  final byte[] tmp = new byte[newLength];
		  Platform.copyMemory(
			buffer,
			Platform.BYTE_ARRAY_OFFSET,
			tmp,
			Platform.BYTE_ARRAY_OFFSET,
			totalSize());
		  buffer = tmp;
		  row.pointTo(buffer, buffer.length);
		}
	}

UnsafeRowWriter的`write`方法就是把数据写到BufferHolder的`buffer`中，然后调用`reset`来触发BufferHolder将数据提交到UnsafeRow的`baseObject`中。BufferHolder的提交过程发生在`grow`方法中，
可以看出BufferHolder实际上只有一个buffer，而且一个Iterator中也只有一个UnsafeRow变量，只是BufferHolder每次生成新的buffer，写入UnsafeRow对象中取出，所以造成一种假象似乎是每个对象都是UnsafeRow。
这部分的逻辑都在代码生成当中，所以很找到，但原理是基本一致的。

![iterator InternalRow][iterator]


	
[1]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_physicalplan.md
[2]:https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html
[ensureDistributionAndOrdering]:../../pic/sort-distribution.png
[execution]:../../example.png
[iterator]:../../pic/Iterator.png



