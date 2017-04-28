# Spark SQL Physical Plan

本文介绍Spark SQL的PhysicalPlan的生成，这一部分主要是基于CBO(Cost Based Optimizer)的优化。

![generate physical plan][physical-plan]

	lazy val sparkPlan: SparkPlan = {
		SparkSession.setActiveSession(sparkSession)
		// TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
		//       but we will implement to choose the best plan.
		planner.plan(ReturnAnswer(optimizedPlan)).next()
	}
	
`SparkSession.setActiveSession(sparkSession)`主要是重新设置SparkSession，因为此时的SparkSession已经经过之前的操作过后发生了改变，必须确保不同线程可以获得这个新的SparkSession，而不是初始化创建的那个。
然后就是`ReturnAnswer(optimizedPlan)`，ReturnAnswer其实一个特殊的LogicalPlan节点，其被插入到优化后的logical plan的顶层。
`planner`用于将LogicalPlan转化为physicalPlan。

	//QueryPlanner
	def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {		
		val candidates = strategies.iterator.flatMap(_(plan))
		val plans = candidates.flatMap { candidate =>
		  val placeholders = collectPlaceholders(candidate)

		  if (placeholders.isEmpty) {
			Iterator(candidate)
		  } else {
			placeholders.iterator.foldLeft(Iterator(candidate)) {
			  case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
				val childPlans = this.plan(logicalPlan)
				candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
				  childPlans.map { childPlan =>
					candidateWithPlaceholders.transformUp {
					  case p if p == placeholder => childPlan
					}
				  }
				}
			}
		  }
		}

		val pruned = prunePlans(plans)
		assert(pruned.hasNext, s"No plan for $plan")
		pruned
	}

首先针对优化后的LogicalPl生成候选PhysicalPlan（就是针对最外层的操作类型生成的PhysicalPlan），这里的候选PhysicalPl针对的是最外层LogicalPlan的转换，对于子查询（LogicalPlan子树）暂时标记为PlanLater。
然后利用`this.plan(logicalPlan)`将标记为PlanLater的子查询递归转化为PhysicalPlan，最后替换掉相应位置的`placeholder`。所以可以发现转化是由顶向下的。这样每层转化都会生成很多PhysicalPlan，
所以总的PhysicalPlan是指数级增加的，越是复杂的SQL语句，产生的可能的PhysicalPlan数量是越多的。所以在这个函数中最后还有一个`prunePlans`用于剪去不合适的plan，从而避免组合爆炸，但是现在还没有实现。

可以发现代码中暂时实现的仅仅是选择这些PhysicalPlan的第一个，并没有进行基于cost的选择，应该不久之后就会改为选择最优的plan了。

## 生成PhysicalPlan的策略（Strategies）

	//SparkPlanner
	def strategies: Seq[Strategy] =
		  extraStrategies ++ (
		  FileSourceStrategy ::
		  DataSourceStrategy ::
		  DDLStrategy ::
		  SpecialLimits ::
		  Aggregation ::
		  JoinSelection ::
		  InMemoryScans ::
		  BasicOperators :: Nil)
		  
`extraStrategies`是由`experimentalMethods.extraStrategies`提供，[Catalyst Logical Plan Optimizer][1]中介绍过该类。DDLStrategy针对DDL语句，DataSourceStrategy和FileSourceStrategy类似，只是针对的数据源是JDBC等外部数据库的数据（非HadoopFsRelation），这里不对以上两个策略做分析。下面一一介绍其他策略。

### FileSourceStrategy

该策略会扫描文件集合，因为它们可能是按照用于指定的列进行partition（按照给定属性划分，文件名是对应值）或bucket（bucket是另一种划分方式，按照给定属性列划分成多个bucket，文件名是bucket编号）。所以通过代码了解到这个策略仅用于HadoopFsRelation（下面的总结在看完代码分析之后会更清楚）。
> 该策略可能发生的几个阶段为：

> 1. 分离filters以用于分别求值，因为不同的filter可能作用于不同的数据；
> 2. 基于现有的投影需要的数据来对数据schema进行剪枝。该剪枝现仅用于顶层column；
> 3. 通过将filters和schema传递到FileFormat中来构造reader函数；
> 4. 使用分片剪枝谓词枚举需要读取的文件；
> 5. 增加必须在扫描之后求值的projection或者filters

> 依照一下算法将文件分配给任务：

> 1. 当表是“bucketed”，按照bucket id把文件组织进正确数目的Partitions；
> 2. 当表不是“bucketed”或“bucketing”被关闭了：
	* 如果文件很大，超出了阈值，将其基于该阈值分成多片；
	* 按照文件大小降序排列；
	* 将排序好的文件按照后面的算法装入buckets；如果现有部分在加入下一个文件之后没有超出阈值，那么就添加，反之就生成新的bucket来添加之。

首先来看PhysicalOperation，因为该策略首先需要依靠该类从LogicalPlan中提取确定的project和filter操作。

	//patterns.scala
	private def collectProjectsAndFilters(plan: LogicalPlan):
		  (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, Map[Attribute, Expression]) =
		plan match {
		  case Project(fields, child) if fields.forall(_.deterministic) =>
			val (_, filters, other, aliases) = collectProjectsAndFilters(child)
			val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
			(Some(substitutedFields), filters, other, collectAliases(substitutedFields))

		  case Filter(condition, child) if condition.deterministic =>
			val (fields, filters, other, aliases) = collectProjectsAndFilters(child)
			val substitutedCondition = substitute(aliases)(condition)
			(fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)

		  case BroadcastHint(child) =>
			collectProjectsAndFilters(child)

		  case other =>
			(None, Nil, other, Map.empty)
		}
		
该方法就是找到所有确定的Project和Filter。Expression.deterministic表示当输入固定的时候，输出也是确定的，不会因为多次运行而改变。所以这里先匹配所有Project表达式中确定的投影变量，
并且通过内联或替换把别名变为最原始的Expression。Filter的操作类似，只是针对的是条件表达式，而非投影表达式。而且二者处理稍有区别，对于条件表达式，替换之后的条件表达式与子树的条件表达式要合并起来，
而投影表达式就直接替换，因为子树的投影表达式只可能会比父节点的多，所以不会产生错误的结果。

该方法执行完之后会有返回“确定的”投影表达式和Filter表达式，以及传入的LogicalPlan。

	//FileSourceStrategy
	object FileSourceStrategy extends Strategy with Logging {
	  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
		case PhysicalOperation(projects, filters,
		  l @ LogicalRelation(fsRelation: HadoopFsRelation, _, table)) =>

		  val filterSet = ExpressionSet(filters)

		  val normalizedFilters = filters.map { e =>
			e transform {
			  case a: AttributeReference =>
				a.withName(l.output.find(_.semanticEquals(a)).get.name)
			}
		  }

		  val partitionColumns =
			l.resolve(
			  fsRelation.partitionSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)
		  val partitionSet = AttributeSet(partitionColumns)
		  val partitionKeyFilters =
			ExpressionSet(normalizedFilters.filter(_.references.subsetOf(partitionSet)))
		  logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")

		  val dataColumns =
			l.resolve(fsRelation.dataSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

		  // Partition keys are not available in the statistics of the files.
		  val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

		  // Predicates with both partition keys and attributes need to be evaluated after the scan.
		  val afterScanFilters = filterSet -- partitionKeyFilters
		  logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

		  val filterAttributes = AttributeSet(afterScanFilters)
		  val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
		  val requiredAttributes = AttributeSet(requiredExpressions)

		  val readDataColumns =
			dataColumns
			  .filter(requiredAttributes.contains)
			  .filterNot(partitionColumns.contains)
		  val outputSchema = readDataColumns.toStructType
		  logInfo(s"Output Data Schema: ${outputSchema.simpleString(5)}")

		  val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
		  logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")

		  val outputAttributes = readDataColumns ++ partitionColumns

		  val scan =
			new FileSourceScanExec(
			  fsRelation,
			  outputAttributes,
			  outputSchema,
			  partitionKeyFilters.toSeq,
			  pushedDownFilters,
			  table)

		  val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
		  val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
		  val withProjections = if (projects == withFilter.output) {
			withFilter
		  } else {
			execution.ProjectExec(projects, withFilter)
		  }

		  withProjections :: Nil

		case _ => Nil
	}

* `ExpressionSet`类其实是用于过滤掉规范化后形式相同的表达式，例如"a+1"和"1+a"规范化后的形式是一样的，所以这算是一步去冗余的操作。`normalizedFilters`的作用是将条件表达式中的属性名统一成输出的属性名。

* `partitionColumns`是将HadoopFsRelation的partition schema（就是那些用于Partition分区的key属性）和resolver传入，从而解析出该HadoopFsRelation的partition属性。这里有个疑问，这一步不应该在解析成Realoved LogicalPlan的时候就做了吗？实际上哪一步的操作有些遗漏，就是针对外部系统的表，
当时只是查询了Catalog有没有，并没有将其相应属性进行连接，变为真正的resolved状态。`partitionSet`作用也是去除重复的属性。

* `partitionKeyFilters`是找到条件表达式中**只**包含partition属性的表达式集合。这样在进行过滤时可以直接通过partition编号进行，从而提高操作的效率。

* `dataColumns`对应的是存储数据的属性，主要是非partition属性，但是如果partition属性有数据，那么也会保留。`dataFilters`是那些只包含非partition属性的filter。

* `afterScanFilters`表示的是经过扫描有才能确定的Filter，例如：`partitionKeyFilters`只需要针对与key值对应的partition即可，但是其他情况必须将所有表都扫描完才能确定。

* 所以`requiredAttributes`是需要的属性，`readDataColumn`是最终需要查询属性中的非partition属性。然后输出相应的Schema信息`outputSchema`。

* `pushedDownFilters `是将`dataFilter`中的filter原先的常量替换成Scala的原生类型（因为Catalyst中的类型不能函数操作，其只是用于表示）。

* 实际上扫描表输出的属性除了`readDataColumn`还有`partitionColumns`，即`outputAttributes`。首先需要强调`requiredAttributes`和`outputAttributes`不同，后者包含了所有的Partition column，前者只包含部分。

* 然后创建扫描HadoopFsRelation中数据的PhysicalPlan节点，`scan`。**那么之前的操作说到底都是在做过滤，就是过滤没必要查询的属性列。**

> 之前这里一直有个疑问，`afterScanFilters`和`dataColumns`是什么关系？其实后者是前者的子集，后者存在的目的是为之后的扫描缩小范围，前者之所以要包含后者是因为有些操作，只有在第二次扫描的时候才能最终确定。
例如：select * from tb1 where a < c and b < c and b = max(c)，令a是partition key，那么第一遍即使传入了b < c and b = max(c)也只是缩小了范围b = max(c)并不能马上计算，因为a < c在之后才能确定，所以由于很难区分`dataFilter`中这些操作，只能统一重新计算，保证正确性。

之后就是将`afterScanFilters`用and连接起来，然后生成新的PhysicalPlan节点`withFilter`，其子节点为`scan`。最后在`withFilter`上加上一个投影节点。

> 可以发现该策略的作用语句很简单，只是select ... from tb1 where ...类型的操作。没有额外的子查询和Aggregate等。

### SpecialLimits

	//SparkStrategies
	object SpecialLimits extends Strategy {
		override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
		  case logical.ReturnAnswer(rootPlan) => rootPlan match {
			case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
			  execution.TakeOrderedAndProjectExec(limit, order, None, planLater(child)) :: Nil
			case logical.Limit(
				IntegerLiteral(limit),
				logical.Project(projectList, logical.Sort(order, true, child))) =>
			  execution.TakeOrderedAndProjectExec(
				limit, order, Some(projectList), planLater(child)) :: Nil
			case logical.Limit(IntegerLiteral(limit), child) =>
			  execution.CollectLimitExec(limit, planLater(child)) :: Nil
			case other => planLater(other) :: Nil
		  }
		  ...
		}
	}

ReturnAnswer这个类在本文开篇介绍过，只是用与封装。省略掉的其他case的处理包含在该种情况中。

> 这里先普及一个概念，即Window function，之前忽略了，这里补上，因为里面会涉及到GlobalLimit的概念。Window操作是在查询结果集上执行的操作，其相对于过去的聚合操作来说，
粒度更细，而且效率更高。例如：SELECT Row_Number() OVER (partition by xx ORDER BY xxx desc) RowNumber中OVER关键字后面的就是是一个window function，因为最后该语句相当于输出每个partition的行数，
当然如果也可以给window其别名，这里就会用到WINDOW关键字，类似于SELECT sum(salary) OVER w, avg(salary) OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);关于window函数的详细知识见[SQL Server中的窗口函数][2]。
GlobalLimit其实就是指window函数中的windows子块，如ROWS，RANGE等。

	//logical.Limit
	def unapply(p: GlobalLimit): Option[(Expression, LogicalPlan)] = {
		p match {
		  case GlobalLimit(le1, LocalLimit(le2, child)) if le1 == le2 => Some((le1, child))
		  case _ => None
		}
	}
	
可以发现该unapply方法比较特殊，因为它必须针对GlobalLimit的子查询是LocalLimit的情况，而且这两个Limit的limit表达式还一样，说明有冗余。case 1中还有一个条件是LocalLimit操作的对象是Sort过的，满足这几点才可以执行`execution.TakeOrderedAndProjectExec`。
该函数的作用其实就是按照LocalLimit子查询的顺序来获取前k（Limit后面的数）条记录。这其实是一步优化，就是如果GlobalLimit和LocalLimit的获取的记录条数相同，因为GlobalLimit的操作是在LocalLimit的基础上执行的，那么无论window函数怎么排序，其实都是不会影响到最终的结果。
例如：

	select count(*) over (sort by b rows 100) from (select * from tb1 sort by c) limit 100 
	==> select count(*) from (select * from tb1 sort by c) limit 100

case 2的区别就是，LocalLimit的子查询是作用在排序上面的投影操作，调用函数同case 1，只是限定了输出的属性，对于没有处理的子树，这里都是用`planLater`进行标记。

case 3就是GlobalLimit的子查询并不满足上面的条件，直接生成CollectLimitExec PhysicalPlan节点，注意上面的TakeOrderedAndProjectExec也是PhysicalPlan节点。

### Aggregation

	//SparkStrategies
	object Aggregation extends Strategy {
		def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
		  case PhysicalAggregation(
			  groupingExpressions, aggregateExpressions, resultExpressions, child) =>

			val (functionsWithDistinct, functionsWithoutDistinct) =
			  aggregateExpressions.partition(_.isDistinct)
			...

			val aggregateOperator =
			  if (functionsWithDistinct.isEmpty) {
				aggregate.AggUtils.planAggregateWithoutDistinct(
				  groupingExpressions,
				  aggregateExpressions,
				  resultExpressions,
				  planLater(child))
			  } else {
				...
				aggregate.AggUtils.planAggregateWithOneDistinct(
				  groupingExpressions,
				  functionsWithDistinct,
				  functionsWithoutDistinct,
				  resultExpressions,
				  planLater(child))
			  }

			aggregateOperator

		  case _ => Nil
		}
	}

首先来看PhysicalAggregation，其与LogicalAggregate不同的地方在于：

1. 给无名grouping表达式命名，从而使这些表达式可以在聚合阶段被引用；
2. 出现多次的Aggregation要去重；
3. 各个aggregation操作本身的计算与最终结果分开的。例如：“count + 1”中的“count”是AggregateExpression，但是最终结果是“count.resultAttribute + 1”。

	//PhysicalAggregation
	type ReturnType =
		(Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression], LogicalPlan)

	  def unapply(a: Any): Option[ReturnType] = a match {
		case logical.Aggregate(groupingExpressions, resultExpressions, child) =>

		  val aggregateExpressions = resultExpressions.flatMap { expr =>
			expr.collect {
			  case agg: AggregateExpression => agg
			}
		  }.distinct

		  val namedGroupingExpressions = groupingExpressions.map {
			case ne: NamedExpression => ne -> ne
			case other =>
			  val withAlias = Alias(other, other.toString)()
			  other -> withAlias
		  }
		  val groupExpressionMap = namedGroupingExpressions.toMap

		  val rewrittenResultExpressions = resultExpressions.map { expr =>
			expr.transformDown {
			  case ae: AggregateExpression =>
				ae.resultAttribute
			  case expression =>

				groupExpressionMap.collectFirst {
				  case (expr, ne) if expr semanticEquals expression => ne.toAttribute
				}.getOrElse(expression)
			}.asInstanceOf[NamedExpression]
		  }

		  Some((
			namedGroupingExpressions.map(_._2),
			aggregateExpressions,
			rewrittenResultExpressions,
			child))

		case _ => None
	}
	
第一句首先是收集不同的聚合操作，例如select sum(*)+count(*),count(*),count(*)+1 from tb1 group by a中实际上只包含两个聚合操作，sum(*)和count(*)。

第二句是对grouping表达式中非NamedExpression作别名处理。第三句就是形成Map[Expression,NamedExpression]映射。

原始的`resultExpressions`是一组可能引用了聚合函数、grouping column值和常量的表达式的集合，当聚合算子输出后，会利用`resultExpressions`生成投影结果。
因此这里会重写结果表达式，即`rewrittenResultExpressions`，从而使其属性与最后投影的输入行匹配。就是把`resultExpressions`的元素类型换成Attribute。
case 2是当表达式不是AggregateExpression时，要与grouping expression中的表达式进行统一（语义相同视为相同，例如：a+1和1+a语义相同，a+b-c和a-c+b相同），以减少计算。

回到Aggregation，将收集到的不同的AggregateExpression划分为两部分，包含DISTINCT关键字的和不包含的。当然包含DISTINCT的AggregateExpression数量不能超过1个。
所以之后的操作就是针对包含一个DISTINCT的aggregation生成一种类型的PhysicalPlan节点，针对不包含DISTINCT的aggregation生成另一种PhysicalPlan节点。

对于无DISTINCT的aggregation实际上最后会判断聚合函数适合用哪种PhysicalPlan，HashAggregateExec还是SortAggregateExec。判断依据就是如果聚合函数的返回类型都是可变的（基本类型，包括浮点类型，都是可变的）并且都是supportPartialAggregate的（不支持只有3种情况，Collect，Hive UDAF和Window function中的聚合函数），那么就用HashAggregateExec。
反之就用SortAggregateExec。原因这里没有搞懂。

对于含有一个DISTINCT的aggregation，首先也是按照无DISTINCT的操作生成一个PhysicalPlan节点，这一步中将DISTINCT操作一定程度上转化为了Group By操作。

	//AggUtils.planAggregateWithOneDistinct
	val partialAggregate: SparkPlan = {
		  val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Partial))
		  val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
		  createAggregateExec(
			requiredChildDistributionExpressions =
			  Some(groupingAttributes ++ distinctAttributes),
			groupingExpressions = groupingExpressions ++ namedDistinctExpressions,
			aggregateExpressions = aggregateExpressions,
			aggregateAttributes = aggregateAttributes,
			initialInputBufferOffset = (groupingAttributes ++ distinctAttributes).length,
			resultExpressions = groupingAttributes ++ distinctAttributes ++
			  aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
			child = child)
	}
	
其中`distinctAttributes`表示DISTINCT操作的属性，例如`Max(DISTINCT a)`中的a。可以发现`requiredChildDistributionExpressions`和`groupingExpressions`都分别添加了DISTINCT作用的属性（表达式）。所以可以说DISTINCT操作被转化为了Group By操作。
然后找到包含DISTINCT的聚合函数，即上面例子中的`Max`。生成对应的Expression和Attribute，最后输出的新的PhysicalPlan节点，其子节点就是之前生成的那个节点，只是聚合操作进行了改变，变为最终需要输出的部分。

### JoinSelection

顾名思义，该策略的目的就是基于Joining key和logical plan的大小选择合适的Physical plan。首先通过ExtractEquiJoinKeys找到equi-join key。然后依照流程选择策略：

1. Broadcast：如果join操作某一侧的表预测大小小于阈值（用户定义SQLConf.AUTO_BROADCASTJOIN_THRESHOLD）或者SQL语句中指明（例如用户可以将`org.apache.spark.sql.functions.broadcast()`应用于DataFrame）要广播，那么就广播这一侧的表。
2. Shuffle hash join：如果每个partition的足够小，适合建立hash表的话，并且没有设置优先使用SortMergeJoin，选择该策略。
3. Sort merge：如果两侧的join key都是可排序的，就用该策略。

如果没有join key，流程如下：

1. BroadcastNestedLoopJoin：如果join操作某侧的表可以被广播。
2. CartesianProduct：用于Inner Join（Inner Join中可以包含除了等号之外的其他比较符，如大于、小于、不等于等），有名字可以猜测应该就是作笛卡尔积。
3. BroadcastNestedLoopJoin：其他。

> 补充：ExtractEquiJoinKeys中会遇到EqualNullSafe，该操作在SQL中就是`<=>`操作，因为有时候无法确保等式两侧的属性在实际操作中会不会出现空，若是这种情况，就用该属性的默认值替换，保证双方都为null的时候返回true，仅一方为null，返回false。

至于表大小的预测方法，总体思路就是根据子树的预测结果进行操作，不同操作的预测方式不同，这里不一一赘述。只讲叶子节点的预测方式，对于LocalRelation，其预测方式就是将输出的每条记录的大小（各单元类型的长度之和）乘以记录数，这个属于元数据可以确定。
对于InMemoryRelation，如果已经实例化就直接partition schema的统计信息计算，反之则直接用一个默认值（可以设置）。这里只介绍这两种。

### InMemoryScans

该策略主要是用于对InMemoryRelation的扫描操作。主要看。

def pruneFilterProject(
      projectList: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      prunePushedDownFilters: Seq[Expression] => Seq[Expression],
      scanBuilder: Seq[Attribute] => SparkPlan): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition: Option[Expression] =
      prunePushedDownFilters(filterPredicates).reduceLeftOption(catalyst.expressions.And)

    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
      val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
      filterCondition.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      val scan = scanBuilder((projectSet ++ filterSet).toSeq)
      ProjectExec(projectList, filterCondition.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }

当不需要复杂的映射操作（即表达式嵌套，如sum(max(a),min(b)），即`AttributeSet(projectList.map(_.toAttribute)) == projectSet`，并且filter所包含的属性时查询（投影）属性的子集。那么只要对Relation进行过滤即可，这样省去了投影操作。
反之，就要在过滤操作上加一个投影节点ProjectExec，不过这里的过滤还包含投影操作涉及到的属性，即`(projectSet ++ filterSet).toSeq`。

### BasicOperators

直接将基本的操作转化为PhysicalPlan节点，没有额外的策略选择。

> 由于有的LogicalPlan同时满足多种策略，所以通常每层分析会有多种策略可供选择，但每种策略只会返回一种。现在的SQL暂时还没有实现基于Cost的策略选择，而且也没有实现剪枝（去除不好的策略，以免组合爆炸），但是这些都在TODO中。
可以发现PhysicalPlan和LogicalPlan节点是很不一样的，LogicalPlan的节点就是操作逻辑，很好理解，但是PhysicalPlan节点是尽量把LogicalPlan中的多个操作合并在一起进行处理，从而减少实际查询的开销。并且和LogicalPlan不同的是，
并没有PhysicalPlan这个类，代码中出现的PhysicalPlan只是泛型的名称，实际上代表PhysicalPlan的就是SparkPlan。

[1]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_optimize.md
[2]:http://www.cnblogs.com/CareySon/p/3411176.html
[physical-plan]:../../pic/physical_plan.png
