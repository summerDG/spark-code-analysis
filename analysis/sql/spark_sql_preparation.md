#Spark SQL 基础知识

本文参考自[Spark Catalyst的实现分析][1]。先上一张所有Catalyst的流程图。虽然本文不会涉及流程，但是之后的分析会以该图为指导。

![Catalyst][calalyst_flow]

本文着重介绍几个SQL中的几个重要概念，不对其分析进行展开。

##Row

表示关系运算的一行输出。其是一个Trait，所以有很多具体实现。实际上本质上来说就是一个数组。但是和RDD不同的是，
RDD中的类型可以是任意的，而DataFrame中每条数据的类型只能是Row。在Spark1.6之后DataFrame就变成了DataSet[Row]的别名。

Row表示的只能是一行结构化数据，非结构化不合法。Row本身有`schema`，用于指明各个字段的类型和列名。
但是支持的数据结构并不是任意的，而是必须继承自DataType，Sparl SQL中已经实现了数据库字段的基本类型。
其也允许继承UserDefinedType来定义自己的类型，这个类中要实现自己的序列化和反序列化操作。如果不定义`schema`
就会使用泛化的Get操作，并且不可以通过列名进行操作。但是它是类型不安全的，因为数据的类型根本不会受到`schema`的约束。

DataSet是Spark1.6之后版本的概念。DataSet和RDD、DataFrame一样，都是分布式数据结构的概念。
区别在于DataSet可以面向特定类型，也就是其无需将输入数据类型限制为Row，或者依赖`Row.fromSeq`将其他类型转换为Row。
但实际上，DataSet的输入类型也必须是与Row相似的类型（如Seq、Array、Product，Int等），最终这些类型都被转化为Catalyst
内部的InternalRow和UnsafeRow。

DataSet的核心概念就是Encoder，这个工具充分利用了隐式转换和上下文界定（过去不了解上下文界定函数中其实有一个默认的参数就是传入一个相应的隐式值，
获取其本身；其都是作为函数的最后一个参数传入的）。例如：

	private[sql] implicit val exprEnc: ExpressionEncoder[T] = encoderFor(encoder)

由于从道理上说，泛型可以传入任意类型，但是实际上的而处理函数不可能实现所有可能，所以存在类型界定。在Java中这个功能就比较弱了，
它只能确定类型的上下界。scala中除了可以限定上下界，还可以利用视图界定和上下文界定。后两者的目的是一样的，就是用于限定特定的类型，
不是上下界之内的，而是隐式定义过的。区别在于前者需要定义隐式转换（类似`implicit ev: A => B`），后者需要定义隐式值（类似`implicit ev: B[A]`）。

下面再说一下Scala中TypeTag这个类（很多地方会用到，参考[这里][2]）。TypeTag是用于解决Scala的泛型会在编译的时候被擦除的问题。这其实也是Java的问题。
为了不被擦除，就用TypeTag这个类来解决。例如：`typeTag[List[Int]]`运行时的值为`TypeTag[scala.List[Int]]`，`typeTag[List[Int]].tpe`和`typeOf[List[Int]]`
的值一样，是`scala.List[Int]`。与TypeTag类似的有ClassTag，但CLassType只包含运行时给定类的类型信息，例如：`ClassTag[scala.List[Int]]`就是`scala.collection.immutable.List`。
`typeTag[T].mirror`可以获得当前环境下的所有可用类型（类似于classloader）。由于Catalyst用到反射机制来解析类型，所以关于Scala的反射机制参考[Scala doc][3]。

回到Encoder，其作用就是将外部类型转化为DataSet内部的InternalRow。但是这个转换是有类型检查的。另外InternalRow还有一个子类，即MutableRow，
而且UnsafeRow也是MutableRow的子类，它即为可修改的InternalRow，在很多地方都会出现这个，原理很简单，支持set等操作而已。


##Expression

在SQL语句中，除了SELECT FROM等关键字以外，其他大部分元素都可以理解为Expression，比如SELECT sum(a), a，其中sum(a)和a都为Expression，这其中当然也包含表名。
每一个DataSet在创建的时候都会有一个对应的ExpressionEncoder，而ExpressionEncoder创建必须得有两个和Expression相关的对象：`serializer: Seq[Expression]`和
`deserializer: Expression`，前者用于将表中一条记录中各个分量解析后转化为Calalyst的InternalRow，后者用于将InternalRow转换为对应类型。
所以Expression还可以表示除表达式之外的类型元素，如属性、常量、行。对于任何一个DataSet[T]，首先会生成一个ExpressionEncoder的隐式值。
生成该隐式值的流程（在ScalaReflection这个工厂中）为：

1. 解析出类型T，这里应该是一个类似于Row或者Product的类型；
2. 通过该类型解析出对应变量，生成对应与该（类数组）变量的Expression，其是一个CreateNamedStruct类型（继承自Expression），
例如：针对_FUNC_(name1, val1, name2, val2, ...)这样一条数据，该对象就可以有效地表示它，并且可以`flatten`成为一组Expression（对应`serializer`），
每一个Expression用于解析一个（namei，vali）；
3. 只要给定目标类型T，那么就一定会生成一个对应的Expression用于将任意的InternalRow转化为该类型的对象；
4. 利用3和4生成的`serializer`和`deserializer`，以及从T获取到的Schema，以及T对应的ClassTag生成ExpressionEncoder对象。

* Expression是一个Tree结构（结构上可以有一个、两个或三个child，也可以没有）。可以通过多级的Child Expression来组合成复杂的Expression。前面提到的对原始数据进行转换就是一个复杂的Expression。
* Expression基本功能是求值，就是`eval`方法，输入InternalRow然后返回结果。
* 既然Expression的功能是求值，那么它就有输入和输出类型的限制。每个Expression都有def dataType: DataType类型变量来表示它的输出类型，以及def checkInputDataTypes(): 
TypeCheckResult函数来校验当前Expression的输入（为Tree结构，那么它的输入即为Child Expression输出）是否符合类型要求。
* Expression功能是针对Row进行加工，但是可以把加工方法分为以下几种
	* 原生的def eval(input: InternalRow = null): Any函数；
	* 对于包含子表达式的Expression（如：UnaryExpression、BinaryExpression、TernaryExpression等），Expression的计算是基于Child Expression计算结果进行二次加工的，
	因此对于这类Expression，对Eval进行默认实现，子类只需要实现函数`def nullSafeEval(input: Any): Any`即可以。
	* Expression也可能是不支持eval的，即Unevaluable类型的Expression，一般有三种情况：1)是真的无法求值，比如处于Unresolved状态的Expression；
	2)是不支持通过eval进行求值，而需要通过gencode的方式来实现Expression功能，涵盖了对全局操作的Expression，例如：Aggravation、Sorting、Count操作;
	3)Expression为RuntimeReplaceable类型（仅有IfNull，NullIf，Nvl和Nvl2），它仅仅是在parser阶段一种临时Expression，在优化阶段，会被替换为别的Expression，因此它本身不需要有执行逻辑，但是得有替换相关的逻辑。
	* Projection类型，它本身不是传统意义上的Expression，但是它可以根据N个Expression，对输入row的N个字段分别进行加工，输出一个新的Row，即Expression的容器。
	
下面对Expression进行分类：
**数据输入**：这部分基本都是继承自LeafExpression，即没有子表达式，用于直接产生数据。

| Name      | 功能描述 |
| --------- | -------- |
| Attribute | Catalyst里面最为重要的概念，可以理解为表的属性，在sql处理各个阶段会有不同的形态，比如UnresolvedAttribute->AttributeReference->BoundReference，后面会具体分析 |
| Literal   | 常量，支持各种类型的常量输入 |
| datetimeExpressions | 对当前时间类型常量的统称（并不包含时间操作），包括`CurrentDate`,`CurrentTimestamp` |
| randomExpressions | 根据特定的随机分布生成一些随机数，主要包括RDG（生成随机分布）|
| 其他一些输入 | 比如获取sql计算过程中的任务对应的InputFileName，SparkPartitionID |

**基本计算功能**：这部分基本都包含子表达式，所以基本都是继承自UnaryExpression、BinaryExpression、BinaryOperator和TernaryExpression。

| Name      | 求值方式 | 功能描述 |
| --------- | :-------: | -------- |
| arithmetic | nullSafeEval | 数学Expression，支持`-`,`+`,`abs`, `+`,`-`,`*`,`/`,`%`,`max`,`min`,`pmod`数学运算符 |
| bitwiseExpressions | nullSafeEval | 位运算数，支持IntegralType类型的`and`,`or`,`not`,`xor`位运算 |
| mathExpressions | nullSafeEval | 数学函数，支持`cos`,`Sqrt`之类30多种,相当于Math包 |
| stringExpressions | nullSafeEval | 字符串函数，支持`Substring`,`Length`之类30多种，相当于String包 |
| decimalExpressions | nullSafeEval | Decimal类型的支持，支持`Unscaled`,`MakeDecimal`操作 |
| datetimeExpressions | nullSafeEval | 时间类型的运算（和上面不同的是，这里指运算）|
| collectionOperations | nullSafeEval | 容器的操作，暂时支持容器`ArrayContains`,`ArraySort`,`Size`，`MapKeys`和`MapValues`5种操作 |
| cast | nullSafeEval | 支持数据类型的转换 |
| misc | nullSafeEval | 功能函数包，支持MD5，crc32之类的函数功能 |

**基本逻辑计算功能**：包括与或非、条件、匹配。

| Name      | 求值方式 | 功能描述 |
| --------- | :-------: | -------- |
| predicates  | eval/nullSafeEval类型 | 支持子Expression之间的逻辑运算，比如`AND`,`In`,`Or`，输出blooean |
| regexpExpressions|nullSafeEval | 支持LIKE相关操作，返回blooean |
| conditionalExpressions | eval | 支持Case（分为CaseWhen和CaseWhenCodegen），If四种逻辑判断运算 |
| nullExpressions | eval/RuntimeReplaceable | 与NULL/NA相关的判断或者IF判断功能，大部分都为RuntimeReplaceable，会被进行优化处理 |

**其他类型**

| Name      | 求值方式 | 功能描述 |
| --------- | :-------: | -------- |
| complexTypeCreator | eval | SparkSql支持复杂数据结构，比如Array，Map，Struct，这类Expression支持在sql语句上生成它们，比如select array。常用于Projection类型。 |
| Generator | eval | 支持flatmap类似的操作，即将Row转变为多个Row，支持Explode和自定义UserDefinedGenerator两种，其中Explode支持将数组和map拆开为多个Row。|

##Attribute

上面已经介绍过，Attribute其实也是一种Expression，继承自NamedExpression，就是带名字的Expression。
Attribute直译为属性，在SQL中，可以简单理解为输入的Table中的字段，Attribute通过Name字段来进行命名。
SQL语句通过Parse生成AST以后，SQL语句中的每个字段都会解析为UnresolvedAttribute，它是属于Attribute的一个子类，比如SELECT a中的a就表示为UnresolvedAttribute("a")。
SQL语句中的`*`，它表示为Star，继承自NamedExpression，它有两个子类：UnresolvedStar和ResolvedStar，二者在analysis.unresolved文件中，但二者其实并没有转换关系，
前者用于AST分析，后者用于查询。

分析需对query的AST加工过程中很重要的一个步骤就是将整个AST中所有Unresolved的Attribute都转变为resolved状态。这个过程在ASTBuilder和Analyzer中配合完成，
前者用于生成unresolved attribute（包括Star和relation等），后者是通过Logic plan对这些unresolved对这些unresolved attribute解析生成固定的AttributeReference（或relation等，针对不同类型最终不一样，该过程目的就是“固定”）。

此外，resolve操作的主要功能就是关联SQL语句所有位置用到的Attribute，即在Attribute的name基础上，指定一个ID进行唯一标示，
**如果一个Attribute在两处被多处被引用，ID即为同一个**。

> Attribute Resolve操作时**从底到顶**来遍历整个AST，每一步都是**根据**底部**已经resloved的Attribute**来给顶部的Attribute赋值，从而保证如果两个Attribute是指向同一个，它们的ID肯定是一样的)。

可以这么理解，做这些事情都是为了优化，物理存储的Table可能有很多Attribute，而通过resolve操作，就指定整个计算过程中需要使用到Attribute，即可以只从物理存储中读取相应字段，
上层各种Expression对这些字段都转变为引用，因此resolve以后的Attribute不是叫做resolvedAttribute,而是叫做AttributeReference。

对于一个中间节点的Expression，如果它对一个Attribute有引用，比如求一个字段值的长度length(a)，这里a经过了UnresolvedAttribute到AttributeReference的转化，但是针对一个输入的Row，
进行lengthExpression计算时，还是无法从AttributeReference中读取相应在Row中的值，为什么？虽然AttributeReference也是Expression，但是它是Unevaluable，为了获取属性在输入Row中对应的值，
需要对AttributeReference再进行一次BindReferences的转化，生成BoundReference，这个操作本质就是将Expression和一个输入Scheme进行关联，Scheme由一组AttributeReference，它们之间是有顺序的，
通过Expression中AttributeReference在Schema AttributeReference组中的Index，并生成BoundReference，在对BoundReference进行eval时候，即可以使用该index获取它在相应Row中的值。

##QueryPlan

如上所言，在SQL语句中，除了SELECT FROM等关键字以外，其他大部分元素都可以理解为Expression，那么用什么来表示剩下的SELECT FROM这些关键字呢？毕竟Expression只是一些Eval功能函数或者代码片段，需要一个东西来串联这些片段，这个东西就是Plan，具体来说是QueryPlan。

QueryPlan就是将各个Expression组织起来，子类有LogicalPlan和PhysicalPlan（源码中没有该类或接口，在plan.physical下有具体形式）。Plan表现形式也是Tree，节点之间的关系可以理解为一种操作次序，比如Plan叶子节点表示从磁盘读取DB文件，而Root节点表示最终数据的输出；下面是Plan最常见的实例截图。

![QueryPlan][plan_img]

用SQL语句来表示这个Plan即为：`SELECT project FROM table, table WHERE filter`。

直观理解，Expression是除了SELECT FROM之外可以看到的Item，Plan就是将Expression按照一定的执行顺序执行。

Expression功能是对输入Row进行加工，输出可能是Any数据类型。而Plan输出类型为def output: Seq[Attribute]表示的一组Attribute，比如上面的Project和Table肯定是输出一个由Seq[Attribute]类型表示的Row，
Filter感觉是输出Ture/False，但是这里说的Plan，而不是Filter类型的Expreesion，Filter类型的Plan会在内部根据Expression计算结果来判断是否返回Row，但是Row返回的类型肯定也是由Seq[Attribute]表示的。
所以说到底Filter还是返回Seq[Attribute]。

同样LogicalPlan从结构上分也有单节点，叶节点，双节点。

Catalyst是对AST树遍历过程中，完成LogicalPlan和所有依赖的Expression的构建，相关逻辑在org.apache.spark.sql.catalyst.parser.AstBuilder以及相关子类中，
整个解析的过程在ParseDriver中，该类中的过程更加宏观清晰。

LogicalPlan也是Tree形结构，其节点分为两种类型：Operator和Command。Command表示无需查询的指令，立即执行，例如：Command可以被用来表示DDL操作。
Operator通常会组成多级的Plan。Operator的类都在basicLogicalOperators下面。这里只取暂时看得懂的。

 Name      |   功能描述
-------- | --------
|`Project`(projectList: Seq[NamedExpression], child: LogicalPlan)|SELECT语句输出操作，其中projectList为输出对象，每一个都为一个Expression，它们可能是Star，或者很复杂的Expression|
|`Filter`(condition: Expression, child: LogicalPlan)|根据condition来对Child输入的Rows进行过滤|
|`Join`(left: LogicalPlan,right: LogicalPlan,joinType: JoinType,condition: Option[Expression])|left和right的输出结果进行join操作|
|`Intersect`(left: LogicalPlan, right: LogicalPlan)|left和right两个Plan输出的rows进行取交集运算。|
|`Except`(left: LogicalPlan, right: LogicalPlan)|在left计算结果中剔除掉right中的计算结果|
|`Union`(children: Seq[LogicalPlan])|将一组Childs的计算结果进行Union联合|
|`Sort`(order: Seq[SortOrder],global: Boolean, child: LogicalPlan)|对child的输出进行sort排序|
|`Repartition`(numPartitions: Int, shuffle: Boolean, child: LogicalPlan)|对child输出的数据进行重新分区操作|
|`InsertIntoTable`(table: LogicalPlan,child: LogicalPlan,...)|将child输出的rows输出到table中|
|`Distinct`(child: LogicalPlan)|对child输出的rows取重操作|
|`GlobalLimit`(limitExpr: Expression, child: LogicalPlan)|对Child输出的数据进行Limit限制|
|`Sample`(child: LogicalPlan,....)|根据一些参数，从child输出的Rows进行一定比例的取样|
|`Aggregate`(groupingExpressions: Seq[Expression],aggregateExpressions: Seq[NamedExpression],child: LogicalPlan)|对child输出row进行aggregate操作，比如groupby之类的操作|
|`Generate`(generator: Generator,join: Boolean,outer: Boolean,ualifier: Option[String],generatorOutput: Seq[Attribute],child: LogicalPlan)|可以用于复杂的查询，将子查询结果以View形式作为输入，输入行以流的形式输入，并以流的形式输出。类似于`flatMap`，但允许将输入与输出连接在一起，也就是将子查询的分析结果作为父查询的输入|
|`Range`(start: Long,end: Long,step: Long,numSlices: Option[Int],output: Seq[Attribute])|对输出数据的范围进行约束|
|`GroupingSets`(bitmasks: Seq[Int],groupByExprs: Seq[Expression],child: LogicalPlan,aggregations: Seq[NamedExpression])|相当于把多个Group By操作合并起来|

下面介绍Command类，这些类都继承自Command，而且数量比Operator多。

| Name      |   功能描述 |
| :-------- | --------|
|`DataBase`操作类|支持ShowDatabase以及UseDatabase以及Create等操作|
|`Table`操作类|多达13种，比如Create，Show，Alter等|
|`View`操作类|CreateViewCommand支持View的创建|
|`Partition`操作类|支持Partition新增删除等操作|
|`Resources`操作类|比如AddJar，AddFile之类的资源操作|
|`Functions`操作类|支持新增函数，删除函数等操作|
|`Cache`操作类|支持对Table进行cache和uncache操作|
|`Set`操作|通过SetCommand执行对参数（任务数和Shuffle的Partition数）进行临时修改|

LogicalPlan需要被转换为最终的PhysicalPlan才能真正具有可执行的能力，而这些Command类型的Plan都是以`def run(sparkSession: SparkSession): Seq[Row]`函数暴露给Spark SQL，
比如通过调用Table的run函数完成Table的创建等操作。

##Tree的操作

TreeNode节点本身类型为Product（在Scala中Product是最基本数据类型之一，其子类包含所有Tuple、List、Option和case类等，如果一个Case Class`继承Product，
那么便可以通过`productElement`函数或者`productIterator`迭代器对`Case Class`的**参数信息**进行索引和遍历），并且所有Expression和Plan都是属于`Product`类型，
因此可以通过TreeNode内部定义的`mapProductIterator`函数对节点参数进行遍历。

对Plan或Expression进行遍历的目的：首先是为了收集一些信息，比如针对Tree进行map/foreach操作；其次是为了对Tree节点内部的信息进行修改，
比如对PlanTree中每个Plan节点内部引用的Attribute进行Revole操作；最后就是为对Tree的数据结构进行修改，比如删除Tree的子节点，以及与子节点进行合并，
比如Catasylt Optitimze就有大量Tree结构的修改。

对Tree进行转换的操作用到的`rule`都是用Scala的偏函数实现的（[偏函数使用][4]，偏函数主要用于匹配）。

对Expression和LogicalPlan的**操作**通常都会被整理到同一个Object中，这个Object中的aplly方法的输入输出类型相同，且其继承自Rule[T]，T标明处理类型（类似于《快学Scala》中的18.12抽象类型中的设计）。

	abstract class Rule[TreeType <: TreeNode[_]] extends Logging {

	  val ruleName: String = {
		val className = getClass.getName
		if (className endsWith "$") className.dropRight(1) else className
	  }

	  def apply(plan: TreeType): TreeType
	}
	
另外可以将一组`Rule`组合为一个`Batch(name: String,rules: Rule[TreeType]*)`并把它封装在`RuleExecutor`中，从而通过`RuleExecutor`将该组`Rule`的可执行接口提供给外部使用，
比如Optimize策略，就是一堆堆的Batch组成。用Batch中的每个Rule（这里想象成对LogicalPlan进行优化）来执行`plan`，直到在到最大允许迭代次数前达到fix point。

但是优化很可能会消耗很长时间，所以每个Batch都有Strategy，其有两个子类Once和FixedPoint，前者表明该Batch只允许执行一次，后者会设定对大迭代次数。

Spark SQL对Plan Tree或者内部Expression Tree的遍历分为几个阶段：

1. 对AST进行Parse操作，生成Unresolve Plan；
2. 对Unresolve Plan进行Analysis(包括Resolve)操作，生成Logical Plan；
3. 对Logical Plan进行Optimize操作，生成Optimized Logical Plan；
4. 以及最后进行Planning操作，生成Physical Plan。

> 这里面的每一阶段都可以简述为应用一组BatchRule来对plan进行加工。

[1]:https://github.com/ColZer/DigAndBuried/blob/master/spark/spark-catalyst.md
[2]:http://stackoverflow.com/questions/12218641/scala-what-is-a-typetag-and-how-do-i-use-it
[3]:http://docs.scala-lang.org/overviews/reflection/overview.html
[4]:http://blog.csdn.net/u010376788/article/details/47206571
[calalyst_flow]:../../pic/Catalyst-Optimizer-diagram.png
[plan_img]:../../pic/plan.png
