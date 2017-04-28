# Catalyst Logical Plan Optimizer

生成Resolved LogicalPlan之后的工作就是执行Logical Plan Optimize。该阶段的主要任务就是对Logical Plan进行剪枝合并等操作，删除无用计算或者对一些计算的多个步骤进行合并。在[Spark Catalyst 分析阶段][1]中也有对LogicalPlan结构的改变，不过那只是将多个LogicalPlan合并成一个，这里是将在一个LogicalPlan的基础上进行结构变化。

![logicalPlan optimizer][logicalPlan_optimizer] 

关于Optimizer：优化包括RBO（Rule Based Optimizer）/CBO(Cost Based Optimizer)，其中Spark Catalyst是属于RBO，即基于一些经验规则（Rule）对Logical Plan的语法结构进行优化；在生成Physical Plan时候，还会基于Cost代价做进一步的优化，比如多表join，优先选择小表进行join，以及根据数据大小，在HashJoin/SortMergeJoin/BroadcastJoin三者之间进行抉择。

优化操作的是在被触发之后才会执行，所以从DataSet的一个Action操作入手，这里就选`collect`。然后调用`QueryExecution.executedPlan`。
QueryExecution中有多个Plan需要Lazy执行，首先是利用`analyzed`生成`withCachedData`，然后利用`withCachedData`生成`optimizedPlan`，这之前的操作都是在LogicalPlan上执行的，并且生成的也都是LogicalPlan。然后就是生成`sparkPlan`，即PhysicalPlan，继而是`executedPlan`。

## withCachedData

在进入LogicalPlan Optimizer之前，首先需要生成`withCachedData`。其作用就是将LogicalPlan中的某些部分替换成已经cached，从而减少不必要的分析运算。可以说这也是Optimizer的一部分。

	//CacheManager
	def useCachedData(plan: LogicalPlan): LogicalPlan = {
		plan transformDown {
		  case currentFragment =>
			lookupCachedData(currentFragment)
			  .map(_.cachedRepresentation.withOutput(currentFragment.output))
			  .getOrElse(currentFragment)
		}
	}
	
操作就是按照前序遍历（`transformDown`是前序遍历对每个节点运行规则，`transformUp`是后序遍历对每个节点运行规则）逐一查询各个LogicalPlan节点的结果是否已经被cached，若是则直接用cached后的结果替换。

	//CacheManager
	def lookupCachedData(plan: LogicalPlan): Option[CachedData] = readLock {
		cachedData.find(cd => plan.sameResult(cd.plan))
	}
	case class CachedData(plan: LogicalPlan, cachedRepresentation: InMemoryRelation)

	//QueryPlan
	def sameResult(plan: PlanType): Boolean = {
		val left = this.canonicalized
		val right = plan.canonicalized
		left.getClass == right.getClass &&
		  left.children.size == right.children.size &&
		  left.cleanArgs == right.cleanArgs &&
		  (left.children, right.children).zipped.forall(_ sameResult _)
	}

先解释一下InMemoryRelation这个类，其包含了一个内存中的Relation的信息，属性列表，存储层级，以及表名，相应RDD，其RDD中数据的类型是CatchedBatch，这个类型包含该条数据的行号，以及数据Buffer，类型是Array[Array[Byte]]，还有一个InternalRow的变量stats指明每条记录中的各个单元是什么类型，因为这里以Array[Byte]来存储每个数据，所以要指明数据类型，从而我们在这里明确InternalRow并没有什么数据，不过就是用于指明数据类型。
`cachedData`类型为ArrayBuffer[CacheData]，相当于一个LogicalPlan和InMemoryRelation的映射。在CacheManager中的`cacheQuery`中可以发现，这里的LogicalPlan就是QueryExecution中的`analyzed`的LogicalPlan，也就是说没有经过优化处理的。*这样的处理也是比较合理的，因为经过优化的LogicalPlan可能根据整体查询的差异而导致即使相同的查询也不能匹配，其次优化是需要处理时间的，每个子查询的优化手段并不能像整体优化那样是确定的，这样会带来很大的开销*。
LogicalPlan的`canonicalized`形态就是将子查询的别名去掉，这样的话不会因为两个LogicalPlan仅仅因为子查询别名不同就判断不相同，然后就是一次比较相关信息。

接下来就是利用`_.cachedRepresentation.withOutput(currentFragment.output)`这句替换原有LogicalPlan子树`currentFragment`的输出。

	//InMemoryRelation
	def withOutput(newOutput: Seq[Attribute]): InMemoryRelation = {
		InMemoryRelation(
		  newOutput, useCompression, batchSize, storageLevel, child, tableName)(
			_cachedColumnBuffers, batchStats)
	}

之前的文章介绍过每个LogicalPlan的输出是一组Attribute，这里生成一个新的Relation，但是Attribute经过了替换。这里要强调一下，InMemoryRelation本身也是一个LogicalPlan的子类，所以替换过程就这样完成了。

## optimizedPlan

	//QueryExecution
	lazy val optimizedPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(withCachedData)

	//SessionState
	lazy val optimizer: Optimizer = new SparkOptimizer(catalog, conf, experimentalMethods)

	//SparkOptimizer
	class SparkOptimizer(
		catalog: SessionCatalog,
		conf: SQLConf,
		experimentalMethods: ExperimentalMethods)
	  extends Optimizer(catalog, conf) {

	  override def batches: Seq[Batch] = super.batches :+
		Batch("Optimize Metadata Only Query", Once, OptimizeMetadataOnlyQuery(catalog, conf)) :+
		Batch("Extract Python UDF from Aggregate", Once, ExtractPythonUDFFromAggregate) :+
		Batch("User Provided Optimizers", fixedPoint, experimentalMethods.extraOptimizations: _*)
	}

可以发现优化规则除了父类的之外分为3个主要部分：

1. 用于只需发现partition层面的元数据就可以回答的查询。应用环境是，当所有被扫描的columns都是partition columns（就是列式存储下，column本身就是一个partition）。查询表达式满足以下条件：
	* 聚合表达式（例中select后面的col）本身就是partition columns，例：SELECT col FROM tbl GROUP BY col；
	* 聚合操作（count，avg，sum等）作用于DISTINCT过的partition columns，例：SELECT col1, count(DISTINCT col2) FROM tbl GROUP BY col1；
	* 在partition columns上的聚合操作是`Max`，`Min`，`First`和`Last`，因为不管包不包含DISTINCT，其结果都一样，例：SELECT col1, Max(col2) FROM tbl GROUP BY col1。

2. 用于Python，不关心。
3. 用户提供的优化器，但是实际上SparkSQL内部有提供一个接口，ExperimentalMethods。这个类下面有`extraStrategies`和`extraOptimizations`两个函数用于提取或设置策略和优化规则。

第一类的方案就是把针对Partitioned表（存储的都是元数据文件，如HDFS的元数据文件或者CataLog表，即元数据表）的节点这种直接用相应的partition values替换为数据库支持的类型。仅仅是做了数据转换。比如上面的操作用都可以用每个Partition的元数据信息回答。

一般规则的优化在Optimizer这个类下面，即上面代码中的`super.batches`，这个`batches`包含的优化涉及到很多大类。

首先“Finish Analysis”并不属于优化范畴，而是属于Analyzer的范畴，保证了结果一致性（正确性，例如将所有的CurrentData进行同步），并进行规范化操作（将子查询别名去除）。

### 优化规则

	 Batch("Union", Once,
		  CombineUnions) ::
		Batch("Subquery", Once,
		  OptimizeSubqueries) ::
		Batch("Replace Operators", fixedPoint,
		  ReplaceIntersectWithSemiJoin,
		  ReplaceExceptWithAntiJoin,
		  ReplaceDistinctWithAggregate) ::
		Batch("Aggregate", fixedPoint,
		  RemoveLiteralFromGroupExpressions,
		  RemoveRepetitionFromGroupExpressions) ::
		Batch("Operator Optimizations", fixedPoint,
		  ...) ::
		Batch("Decimal Optimizations", fixedPoint,
		  DecimalAggregates) ::
		Batch("Typed Filter Optimization", fixedPoint,
		  CombineTypedFilters) ::
		Batch("LocalRelation", fixedPoint,
		  ConvertToLocalRelation,
		  PropagateEmptyRelation) ::
		Batch("OptimizeCodegen", Once,
		  OptimizeCodegen(conf)) ::
		Batch("RewriteSubquery", Once,
		  RewritePredicateSubquery,
		  CollapseProject) :: Nil
	
可以看到很多优化规则组成的Batch，[Spark-Catalyst Optimizer][2]中介绍了很大一部分。这里修改部分优化规则介绍，并且补充其他规则。

1. OptimizeIn作用有两点：
	* 消除IN操作的序列中的相同元素，例如将In (value, seq[Literal])转为In (value, ExpressionSet(seq[Literal]))
	* 如果消除重复后的序列的元素数目超过`conf.optimizerInSetConversionThreshold`（默认10），转化为INSET操作，即自动将ExpressionSet转换为Hashset，提高IN操作的性能。
2. ColumnPruning涉及到的逻辑很多，这里主要分为：
	* 若子查询中的Project/Aggregate/Expand操作包含最终Project操作中不包含的属性，剪去多余部分
	* 若子操作中包含DeserializeToObject/Aggregate/Expand/Generate中没有的属性，剪去多余部分
	* 对于Generate操作，如果其子树不出现在Generate输出中，并且最终的Project的属性集还是Generate输出属性的子集，那么说明最终都没有用到Generate子树的那部分输出，这里就令Generate输出不与输入子树连接。
	* 对于Left-Semi-Join，将右边的子查询分量的无关属性去除，即只保留其用于Join的属性
	* 对于Project(_, child)操作，只保留child中与Project有关的属性，依据类型不同，部分情况下甚至可以只求child
3. CombineTypedFilters和CombineFilters的操作类似，区别在于TypedFilter属于一种特殊的Filter，其条件函数可以自己定义，只要是一个以行未输入，返回boolean的函数即可，这个规则合并的是条件函数
4. CombineUnions针对多个union操作进行合并，例如：(select a from tb1) union (select b from tb2) union (select c from tb3) => select a,b,c from union(tb1,tb2,tb3)
5. OptimizeSubqueries针对所有子查询做Optimizer中优化规则
6. RemoveLiteralFromGroupExpressions是将所有Group by中的表达式中的常量移除掉，因为其不会影响结果
7. RemoveRepetitionFromGroupExpressions是将Group by中的表达式中相同的表达式移除掉
8. PushProjectionThroughUnion用于将Project向下移动到每个子Union表达式，从而提早缩小范围
9. ReorderJoin，把所有的条件表达式分配到join的子树中，使每个子树至少有一个条件表达式。重排序的顺序依据条件顺序，例如：select * from tb1,tb2,tb3 where tb1.a=tb3.c and tb3.b=tb2.b，那么join的顺序就是join(tb1,tb3,tb1.a=tb3.c)，join(tb3,tb2,tb3.b=tb2.b)
10. EliminateOuterJoin，这条规则是尽量将fullOuterJoin转化为left/right outer join，甚至是inner join，**null-unsupplying谓词表示，如果有null作为输入则返回false或null，也就是说改谓词不支持包含null的输入**，包含5种情况：
	* 对于full outer，如果左边子树的谓词是null-unsupplying，full outer -> left outer；反之，如果右边有这种谓词，full outer -> right outer；若两边都有，full outer -> inner
	* 对于left outer，若右边子树包含null-unsupplying，left outer -> inner
	* 对于right outer，若左边子树包含null-unsupplying，right outer -> inner
11. InferFiltersFromConstraints是将总的Filter中相对于子树约束多余的约束删除掉，只支持inter join和leftsemi join，例如：select * from (select * from tb1 where a) as x, (select * from tb2 where b) as y where a, b ==> select * from (select * from tb1 where a) as x, (select * from tb2 where b) as y
12. FoldablePropagation，先对有别名的可折叠表达式中的属性和其别名建立索引，然后将LogicalPlan中的所有该属性的节点用其别名代替
13. ConstantFolding，将可以静态直接求值的表达式直接求出常量，Add(1,2)==>3
14. ReorderAssociativeOperator，将与整型相关的可确定的表达式直接计算出其部分结果，例如：Add(Add(1,a),2)==>Add(a,3)；与ConstantFolding不同的是，这里的Add或Multiply是不确定的，但是可以尽可能计算出部分结果
15. RemoveDispensableExpressions，这个规则仅仅去掉没必要的节点，包括两类：UnaryPositive和PromotePrecision，二者仅仅是对子表达式的封装，并无额外操作
16. EliminateSorts，首先明确sort by默认只保证partition有序，只有在设置全局有序的情况下才保证全局有序；该规则是消除没有起作用的排序算子，因为排序算子可能是确定的（语法正确，逻辑有问题），那么这样的排序算子就没有意义，例如：select a from tb1 sort by 1+1 ==> select a from tb1
17. RewriteCorrelatedScalarSubquery，ScalarSubquery指的是只返回一个元素（一行一列）的查询，如果Filter，Project，Aggregate操作中包含相应的ScalarSubquery，就重写之，思想就是因为ScalarSubquery结果很小，可以过滤大部分无用元素，所以优先使用left semi join过滤：
	* Aggregate操作，例如select max(a), b from tb1 group by max(a) ==> select max(a), b from tb1 left semi join max(a) group by max(a)，这样先做join，效率要比先做group by操作效率高
	* Project操作，例如select max(a), b from tb1 ==> select max(a), b from tb1 left semi join max(a)
	* Filter，例如select b from tb1 where max(a) ==> select b (select * from tb1 left semi join max(a) where max(a))
18. EliminateSerialization，很多操作需要先序列化，然后反序列化，如果反序列化的输出类型和序列化前的输入类型相同，就省略序列化和反序列化的操作
19. RemoveAliasOnlyProject，消除仅仅由子查询的输出的别名构成的Project，例如：select a from (select a from t) ==> select a from t
20. OptimizeCodegen，对于特定的情况，将一部分查询生成整块的代码，而不是通过一个个算子进行操作，参考[Apache Spark as a Compiler: Joining a Billion Rows per Second on a Laptop][3]
21. RewritePredicateSubquery，将EXISTS/NOT EXISTS改为left semi/anti join形式，将IN/NOT IN也改为left semi/anti join形式
22. ConvertToLocalRelation，将Project中的每个无需数据交换的表达式应用于LocalRelation中的相应元素，例如select a + 1 from tb1转为一个属性名为“a+1”的relation，并且tb1.a的每个值都是加过1的
23. PropagateEmptyRelation，针对有空表的LogicalPlan进行变换
	* 如果Union的子查询都是空，其结果也为空；
	* 如果Join的子查询存在空表，
		- `Inner => empty(p)`，
		- `LeftOuter | LeftSemi | LeftAnti if isEmptyLocalRelation(p.left) => empty(p)`，
		- `RightOuter if isEmptyLocalRelation(p.right) => empty(p)`，
		- `FullOuter if p.children.forall(isEmptyLocalRelation) => empty(p)`
	* 对于单节点Plan，若其子节点为空表，则整体为空表
24. DecimalAggregates应该是用于加速浮点数计算的，因为浮点数计算要控制精度（往往需要通过扩充长度来保证精度），但是一定范围内可以不控制，即不需要每一步的浮点运算都控制精度
25. PushPredicateThroughJoin是将Filter中的条件下移到Join算子中，详情见[Spark SQL Join 上][4]。

[1]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_parser.md
[2]:https://github.com/ColZer/DigAndBuried/blob/master/spark/spark-catalyst-optimizer.md
[3]:https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html
[4]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_join_1.md
[logicalPlan_optimizer]:../../pic/Optimizer-diagram.png
