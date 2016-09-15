#Spark Catalyst 分析阶段

本文从使用者的视角，一步步深入SQL的分析，这部分从SQL语句开始，以LogicalPlan为输出。

![catalyst-analysis][analysis]

我们以官方的一段代码作为讲解的流程。

	import org.apache.spark.sql.SparkSession

	val spark = SparkSession
	  .builder()
	  .appName("Spark SQL Example")
	  .config("spark.some.config.option", "some-value")
	  .getOrCreate()

	// For implicit conversions like converting RDDs to DataFrames
	import spark.implicits._

	case class Person(name: String, age: Long)

	val peopleDF = spark.sparkContext
	  .textFile("examples/src/main/resources/people.txt")
	  .map(_.split(","))
	  .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
	  .toDF()
	peopleDF.createOrReplaceTempView("people")
	val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
	
##解析每条记录的类型

SparkSession是DataSet和DataFrame编程的入口，Builder可以用于在REPL或notebooks中配置环境。第一句的重点是获取到当前环境的SparkSession。
第二步就是引入Spark SQL中的隐式转换和隐式值（RDD.toDF和toDS等操作都是通过隐式转换实现的）。这里虽然是DataFrame，但是之前说过在2.0.0
中DataFrame就是DataSet[Row]，所以不影响我们分析DataSet。`toDF`和`toDS`都是使用如下隐式转换。

	implicit def rddToDatasetHolder[T : Encoder](rdd: RDD[T]): DatasetHolder[T] = {
		DatasetHolder(_sqlContext.createDataset(rdd))
	}
	case class DatasetHolder[T] private[sql](private val ds: Dataset[T]) {
	  def toDS(): Dataset[T] = ds
	  def toDF(): DataFrame = ds.toDF()
	  def toDF(colNames: String*): DataFrame = ds.toDF(colNames : _*)
	}
	
可以发现是RDD到DataSet的转换调用的是SQLContext的`createDataset`方法。但预先必定没有Encoder[Person]的隐式值，隐式转换是通过

	implicit def newProductEncoder[T <: Product : TypeTag]: Encoder[T] = Encoders.product[T]
	
因为Person是case class，所以是Product类型。进入Encoder的`product`，然后进入ExpressionEncoder的`apply`方法。这里就用到了[Spark SQL 基础知识][1]中的提到的反射机制。

	def apply[T : TypeTag](): ExpressionEncoder[T] = {
	    val mirror = typeTag[T].mirror
	    val tpe = typeTag[T].tpe
	    val cls = mirror.runtimeClass(tpe)
	    val flat = !ScalaReflection.definedByConstructorParams(tpe)
	
	    val inputObject = BoundReference(0, ScalaReflection.dataTypeFor[T], nullable = true)
	    val nullSafeInput = if (flat) {
	      inputObject
	    } else {
	      AssertNotNull(inputObject, Seq("top level non-flat input object"))
	    }
	    val serializer = ScalaReflection.serializerFor[T](nullSafeInput)
	    val deserializer = ScalaReflection.deserializerFor[T]
	
	    val schema = ScalaReflection.schemaFor[T] match {
	      case ScalaReflection.Schema(s: StructType, _) => s
	      case ScalaReflection.Schema(dt, nullable) => new StructType().add("value", dt, nullable)
	    }
	
	    new ExpressionEncoder[T](
	      schema,
	      flat,
	      serializer.flatten,
	      deserializer,
	      ClassTag[T](cls))
	}

`flat`用于判断这个类是不是完全由成员变量构造，如果是就将各个成员变量解析为属性，反之就抛异常。进入`ScalaReflection.serializerFor`，
看解析过程。针对case class，我们看`serializerFor`（private）的相应情况。

	private def serializerFor(
		  inputObject: Expression,
		  tpe: `Type`,
		  walkedTypePath: Seq[String]): Expression = ScalaReflectionLock.synchronized {
		  ...
		  tpe match {
		  ...
			  case t if definedByConstructorParams(t) =>
				val params = getConstructorParameters(t)
				val nonNullOutput = CreateNamedStruct(params.flatMap { case (fieldName, fieldType) =>
				  if (javaKeywords.contains(fieldName)) {
					throw new UnsupportedOperationException(s"`$fieldName` is a reserved keyword and " +
					  "cannot be used as field name\n" + walkedTypePath.mkString("\n"))
				  }

				  val fieldValue = Invoke(inputObject, fieldName, dataTypeFor(fieldType))
				  val clsName = getClassNameFromType(fieldType)
				  val newPath = s"""- field (class: "$clsName", name: "$fieldName")""" +: walkedTypePath
				  expressions.Literal(fieldName) :: serializerFor(fieldValue, fieldType, newPath) :: Nil
				})
				val nullOutput = expressions.Literal.create(null, nonNullOutput.dataType)
				expressions.If(IsNull(inputObject), nullOutput, nonNullOutput)
			...
			}
	}

通过反射机制，从类型中解析出各个参数的名字和对应的类型，并且判断参数名是否合法。`Invoke`方法，传入被转化为Expression的类、函数名和对应返回类型，从而有效的调用对应函数。
由于Scala中的成员变量名也可以作为函数名传入，所以这里相当于获取成员变量。这其实是一个绑定的过程，即直接从原有类中读取数据，注意这里并没有把这个类型（Person）转化为其他变量，
而只是将属性名和Person的具体成员变量调用函数进行了绑定，所以读取数据依然是从Person中读取。还可以发现这是个递归的过程，因为可能成员变量类型依然不是Int，String等基本类型，那就要继续解析。
`expressions.Literal(fieldName) :: serializerFor(fieldValue, fieldType, newPath) :: Nil`。当某些函数的返回值因为擦除变成Object类型之后，需要生成代码把结果强制转换为运行类型，这部分代码在
Invoke的`doGenCode`方法中。

所以解析完之后，`serializer`就是将类中各成员变量名映射到了自定义类型的成员变量获取函数中，可以通过`serializer`可以将自定义类型中的数据提取出来进行操作。
反之可以猜想到`deserializer`就是将成员变量的赋值函数映射到各成员变量名，从而可以将处理完的数据完整写入到自定义类型中。当然这二者同样具备序列化和反序列化的功能。

> 想想Java就不能处理这样的问题，重点在于Scala支持类型擦除之后还原（TypeTag的功劳），而且类似于Bash脚本的写法，使得代码生成相当方便。不过真的很佩服Spark工程师想到利用反射，以及用Expression绑定
Schema，从而达到支持任意类型的目的。这部分代码很多，但总体思路这里已经介绍清楚。再多说一句，现在版本的Scala支持一个类中最多允许有22个field，多了的话就得继承Product了。

之后就是利用反射生成Schema，同样也是递归过程，与`serializer`和`deserializer`的区别在于其不用绑定读取或复制函数，仅仅是成员变量名和类型名的对应关系。

最后就是利用上面生成的对象构造ExpressionEncoder，每个DataSet[T]中记录的类型T都会有一个ExpressionEncoder。

##生成DataSet

Encoder生成之后，关注DataSet的生成。

	def createDataset[T : Encoder](data: RDD[T]): Dataset[T] = {
		Dataset[T](self, ExternalRDD(data, self))
	}
	
主要看ExternalRDD，其实这个类的作用就是将RDD转换为LogicalPlan的节点（很明显应该是叶子节点），用于扫描RDD数据。在生成ExternalRDD之前先关注一下`CatalystSerde.generateObjAttr`，
该方法主要是调用AttributeReference的`apply`方法生成一个对属性的引用，这个类型的作用有：

1. 判断两个引用是否指向同一个属性，因为每个属性都只有一个ID；
2. 判断属性是否相同；
3. 更换属性名；
4. 更换修饰符名，例如：tableName.name和subQueryAlias.name中tableName和subQueryAlias都是修饰符名。

ExternalRDD的作用较为简单：

1. 生成本对象的拷贝，但是这个拷贝只是新生成对应AttributeReference的拷贝，对应rdd并不是拷贝。也就是表明这只是用于构建LogicalPlan，因为Plan中可能会多次
用到同一Attribute的引用进行不同操作，甚至改变结构，但这些操作最终都是作用到同一个RDD的；
2. 检验与另一个Plan对应的RDD是否是同一个（似乎仅用于Physical Plan阶段）；

ExternalRDD本身是一个LogicalPlan节点的子类，并且是叶子节点，这很容易解释通，因为这是没有任何查询逻辑，所以它应该被当做叶子节点来输入数据，以供中间节点进行处理。

到此，DataSet生成完毕。但是实际上所有真正的操作都是Lazy的，只有在触发的时候，才会执行QueryPlan。也就是说这里的`toDF`和`toDS`操作都只是转换操作。
`createTempView`操作是Command操作，所以可以立即执行，就是给这个DataSet起别名（视图名，其生命周期由SparkSession决定），当然该操作会验证这个名字全局唯一。

##Query语句解析

`spark.sql(...)`方法首先是调用`SparkSqlParser.parsePlan`来解析这条查询语句。实际调用的是`AbstractSqlParser.parse`。

	protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
		val lexer = new SqlBaseLexer(new ANTLRNoCaseStringStream(command))
		lexer.removeErrorListeners()
		lexer.addErrorListener(ParseErrorListener)

		val tokenStream = new CommonTokenStream(lexer)
		val parser = new SqlBaseParser(tokenStream)
		parser.addParseListener(PostProcessor)
		parser.removeErrorListeners()
		parser.addErrorListener(ParseErrorListener)

		try {
		  try {
			parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
			toResult(parser)
		  }
		  catch {
			case e: ParseCancellationException =>
			  tokenStream.reset() // rewind input stream
			  parser.reset()
			  parser.getInterpreter.setPredictionMode(PredictionMode.LL)
			  toResult(parser)
		  }
		}
		catch {
		  ...
		}
	}

其实这部分的代码如果了解Antlr 4 的解析过程的话会很容易懂，可以参考[Antlr v4入门教程和实例][2]，如果想深入了解参考[ANTLR 4权威参考读书笔记][3]。
Antlr首先对字符串进行词法解析，即`lexer`，这里用Spark本身的`ParseErrorListener`替换了原有的词法错误监听器，其实作用就是将过去的错误类型转化为异常信息。
利用`lexer`生成token流（符号流）。然后利用token流进行`parse`过程，生成语法树。`parse`过程中加入Spark自己的解析器`PostProcessor`针对特殊情况做处理，例如：
将标识符（表名或属性名）中的两个``“`”``换做单个（因为不同数据库操作人员的习惯不同），以及将所有非保留字（select，where等）的token全部当做标识符。
之后设置语法树生成策略（SLL或LL，前者快但能力较弱，没具体了解）。重点进入`toResult`方法，分析其解析过程。

	//AbstractSqlParser
	override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
		astBuilder.visitSingleStatement(parser.singleStatement()) match {
		  case plan: LogicalPlan => plan
		  case _ =>
			val position = Origin(None, None)
			throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
		}
	}

很明显该过程就会生成LogicalPlan。`parser.singleStatement()`生成对应语法树，`singleStatement`规则文件中最顶层的结构名（自定义）。
进入`ASTBuilder.visitSingleStatement`。

	override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
		visit(ctx.statement).asInstanceOf[LogicalPlan]
	}

visit是父函数在运行时生成的，所以代码中没有显示其位置。将singleStatement中的statement提取出来，然后按照层次一层一层解析。
例子`SELECT name, age FROM people WHERE age BETWEEN 13 AND 19`中的变量首先是查询语句。

	//AstBuilder
	override def visitQuerySpecification(
		  ctx: QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
		val from = OneRowRelation.optional(ctx.fromClause) {
		  visitFromClause(ctx.fromClause)
		}
		withQuerySpecification(ctx, from)
	}
	override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
		val from = ctx.relation.asScala.map(plan).reduceLeft(Join(_, _, Inner, None))
		ctx.lateralView.asScala.foldLeft(from)(withGenerate)
	}

第一句获取到from之后语法块（例如表名，View名或者子查询）的LogicalPlan节点，如果有多个对象，会调用Join方法，注意这步并没有进行Join计算，仅仅是确定了其运算逻辑。然后交给`withQuerySpecification`处理。这个函数逻辑比较多，这里只挑例子中对应的成分分析。

	//AstBuilder
	private def withQuerySpecification(
		  ctx: QuerySpecificationContext,
		  relation: LogicalPlan): LogicalPlan = withOrigin(ctx) {
		import ctx._

		// WHERE
		def filter(ctx: BooleanExpressionContext, plan: LogicalPlan): LogicalPlan = {
		  Filter(expression(ctx), plan)
		}
		val expressions = Option(namedExpressionSeq).toSeq
		  .flatMap(_.namedExpression.asScala)
		  .map(typedVisit[Expression])

		val specType = Option(kind).map(_.getType).getOrElse(SqlBaseParser.SELECT)
		specType match {
		  ...

		  case SqlBaseParser.SELECT =>
			...
			val withLateralView = ctx.lateralView.asScala.foldLeft(relation)(withGenerate)
			// Add where.
			val withFilter = withLateralView.optionalMap(where)(filter)
			
			val namedExpressions = expressions.map {
				case e: NamedExpression => e
				case e: Expression => UnresolvedAlias(e)
			}
			val withProject = if (aggregation != null) {
			  withAggregation(aggregation, namedExpressions, withFilter)
			} else if (namedExpressions.nonEmpty) {
			  Project(namedExpressions, withFilter)
			} else {
			  withFilter
			}
			...
		}
	}

后面的解析基于前面的节点，例如首先就是解析View量，然后就是过滤语句，之后是属性名（或者Aggregate操作），接着是投影，之后还有having、distinct等操作。
可以发现这个逻辑是很合理的，因为View量（在不代表子查询的情况下，是最初的输入）是数据输入，之后经过过滤确定出处理的属性，然后获取到针对该属性的Aggregate操作（或就是本身），
然后就是投影输出。

在[Spark SQL 基础知识][1]中谈到Generate是将一组数据的分析结果与当前的分析拼接在一起。但是这里子查询还没有Resolve，该Generator是Unresolved的，暂时仅用于拼接。这里会发现在分析FROM块的时候也会用与View名拼接，
那么这里的处理和那里有什么不同呢？举个例子

	...（SELECT * FROM (table1,table2,lateralView1)） lateralView2 ...

`visitFromClause`仅用于table1和2的Join结果与lateralView1进行拼接，后者用于外层拼接，也就是将子查询的输出与该lateralView2进行拼接。

然后调用`val withFilter = withLateralView.optionalMap(where)(filter)`，当存在where参数的时候，将WHERE后边的条件块映射到名为`withFilter`的LogicalPlan节点中，并且将`withLateralView`作为子节点为Filter提供输入。
这里的Where块中的booleanExpression是Predicated（谓词，支持BETWEEN、IN、LIKE、RLIKE和IS NULL及其反义操作），所以进入withPredicated可以看具体的解析过程。

	ctx.kind.getType match {
		  case SqlBaseParser.BETWEEN =>
			// BETWEEN is translated to lower <= e && e <= upper
			invertIfNotDefined(And(
			  GreaterThanOrEqual(e, expression(ctx.lower)),
			  LessThanOrEqual(e, expression(ctx.upper))))
			  ...
		}

最后我们发现BTWEEN操作被转化为类似于`e >=ctx.lower && e <= ctx.upper`的操作。

之后的投影（Project）操作类似，都可以总结为获取节点中的Expression用于运算，然后连接LogicalPlan节点。

> Expression包含于LogicalPlan节点中，其相当于该节点的计算单元，而LogicalPlan节点之间的联系可以看做是为计算单元提供输入输出接口。

![relation between expression and logicalPlan][expresion-logical]

生成LogicalPlan之后，就需要传给`DataSet.ofRows`。

	//DataSet object
	def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
		val qe = sparkSession.sessionState.executePlan(logicalPlan)
		qe.assertAnalyzed()
		new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
	}
	
首先就是调用`executePlan`来执行logicalPlan，生成QueryExecution对象，但实际上QueryExecution里边的分析操作都是Lazy的，所以可以说这里返回的就是个“准备就绪的机器”，等待触发。
第二句用于检查是否有不支持的操作，例如分析的table不存在（或没有计算出来），Attribute不存在等，以便提早终止错误代码。

然后调用QueryExecution的分析`analyzed`开始执行。

	//QueryExecution
	lazy val analyzed: LogicalPlan = {
		SparkSession.setActiveSession(sparkSession)
		sparkSession.sessionState.analyzer.execute(logical)
	}
	
`analyzer`本身也是Lazy的，所以会调用Analyzer的`execute`方法将Unresolved的Attitude和Relation，通过CataLog转化为真正操作用的类型对象。

##Analyzer

首先看什么是Catalog。它是一个SessionCatalog类，用于维护Spark SQL中表和数据库的状态，它可以和外部系统（如Hive）进行连接，从而获取到Hive中的数据库信息。
之前提到的利用`createTempView`生成表名，这个函数中同时将表名（View名）信息注册到了Catalog中。

创建Analyzer对象的时候，可以对`extendedResolutionRules`进行重写，该规则是用户额外添加的规则，用于解析。还有一个可以重写的field，就是`extendedCheckRules`，
其用于添加用于检测合法性的规则。

Analyzer中包含大量的规则，共分为6类，最重要的两类是：替换（Substitution）和解析（Resolution）。规则本身就是一个方法集（工厂类），从而实现对LogicalPlan的转换。

###Substitution

`CTESubstitution`规则是将CTE定义（就是WITH块）的子查询替换为可处理LogicalPlan，由于CTE定义打乱了语法树的结构（从左到右解析根本没本法直接将CTE定义的子块加到语法树），所以此处要将CTE定义的子块重新按照索引加入到整个查询的LogicalPlan中，并且将所有WITH块中生成的relation解析为Resolved状态。

> 例如：`WITH q as(SELECT time_taken FROM idp WHERE time_taken=15) SELECT * FROM q; `
> 在没有运行该条规则的时候会存在两个LogicalPlan树，分别对应WITH块和`SELECT * FROM q`，然后该规则的作用就是以q为链接依据，将这两个LogicalPlan进行合并。那么WITH块中的查询就应该作为后边FROM块中的子树。

`WindowsSubstitution`规则做的工作类似于`CTESubstitution`，只是语法定义与CTE不同。

> Substitution做的工作是对LogicalPlan的结构做改变，而Resolution的工作只是将原有的节点解析为实体（因为语法解析后的表名仅仅是一个名字，并没有真正地与DataSet建立联系）。

###Resolution

这些规则是有序的，打乱了就很可能导致出错或者短时间内运行不完。

1. 首先第一条规则是`ResolveTableValuedFunctions`，即类似于`range(start, end, step)`的语句。其处理流程就是先匹配函数名（暂时只有range），匹配到之后就可以知道各参数的类型。
然后将之前解析的各参数（或表达式参数）的类型强制转换为expectedType。

2. 第二条规则是`ResolveRelations`，顾名思义，就是将relation的状态解析为resolved，解析为resoleved的过程其实就是将表名变为具体的DataSet实体。

3. 第三条规则是`ResolveReferences`，其将UnresolvedAttribute解析为连向子DataSet中具体属性的引用，即AttributeReference。但AttributeReference本身继承自Unevaluable，所以并未求值。
那么它是怎么将属性名和具体属性联系起来的呢？因为解析每个DataSet的时候会注册很多属性，这个属性是包含具体内容的实体。所以会到注册的表里去查询匹配，从而将UnresolvedAttribute中的属性名与对应Attribute做映射。
具体实现在`resolveAsTableColumn`下。

		//LogicalPlan
		protected def resolve(
			  nameParts: Seq[String],
			  input: Seq[Attribute],
			  resolver: Resolver): Option[NamedExpression] = {
			var candidates: Seq[(Attribute, List[String])] = {
			  // If the name has 2 or more parts, try to resolve it as `table.column` first.
			  if (nameParts.length > 1) {
				input.flatMap { option =>
				  resolveAsTableColumn(nameParts, resolver, option)
				}
			  } else {
				Seq.empty
			  }
			}
			if (candidates.isEmpty) {
			  candidates = input.flatMap { candidate =>
				resolveAsColumn(nameParts, resolver, candidate)
			  }
			}

			def name = UnresolvedAttribute(nameParts).name

			candidates.distinct match {
			  case Seq((a, Nil)) => Some(a)

			  case Seq((a, nestedFields)) =>
				
				val fieldExprs = nestedFields.foldLeft(a: Expression)((expr, fieldName) =>
				  ExtractValue(expr, Literal(fieldName), resolver))
				Some(Alias(fieldExprs, nestedFields.last)())

			  ...
			}
		}
		
		private def resolveAsTableColumn(
			  nameParts: Seq[String],
			  resolver: Resolver,
			  attribute: Attribute): Option[(Attribute, List[String])] = {
			assert(nameParts.length > 1)
			if (attribute.qualifier.exists(resolver(_, nameParts.head))) {
			  // At least one qualifier matches. See if remaining parts match.
			  val remainingParts = nameParts.tail
			  resolveAsColumn(remainingParts, resolver, attribute)
			} else {
			  None
			}
		}
	
输入参数中的`nameParts`表示查询语句中的属性名，为什么是序列类型呢？因为有`tableName.colName.fieldName`的形式。
`attribute`表示可能的属性，因为最先不知道的时候只能根据最开头的名字（表名tableName）一个个去排查。然后调用`resolveAsColumn`去验证是否该`attribute`的名字与colName是否匹配，若是匹配，就返回该`attribute`和需要的其他字段（如fieldName）的映射。
当然还有解析不到的情况，这不是说明这个属性不存在，因为上一步是针对`tableName.colName...`的形式，然后针对直接是`colName...`形式匹配。

最后如果没有其他字段（就是内嵌的属性名）就直接返回该DataSet的Attrubute。反之则将该属性类型具体的成员变量的提取函数与`tableName.colName.fieldName`作联系起来，那么使用`tableName.colName.fieldName`就相当于直接获取`colName`对象具体的成员变量。

其他的规则类似，都是将Unresolved的LogicalPlan节点与具体操作或者实体进行对应。如图所示：

![unresolved-to-resolved][generateLogicalPlan]

***

> `execute`方法在RuleExecutor中，主要作用就是利用Analyzer中的规则集合Batches来处理LogicalPlan。处理方式在[Spark SQL 基础知识][1]中有提到，就是不断应用规则达到Fixed Point（可以设置策略来保证有限时间以内）。



[1]:https://github.com/summerDG/spark-code-ananlysis/blob/master/analysis/sql/spark_sql_preparation.md
[2]:http://blog.csdn.net/dc_726/article/details/45399371
[3]:http://codemany.com/tags/antlr/
[analysis]:../../pic/Catalyst-analysis.png
[expresion-logical]:../../pic/Expression_LogicalPlan.png
[generateLogicalPlan]:../../pic/generate_LogicalPlan.png
