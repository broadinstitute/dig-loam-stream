package loamstream.model

import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import java.nio.file.Paths
import loamstream.tools.klusta.KlustaKwikKonfig

/**
 * @author clint
 * date: Jun 8, 2016
 */
sealed trait DAG { self =>
  import DAG._
  
  type T <: DAG
  
  def id: LId
  
  def isLeaf: Boolean
  
  def leaves: Set[DAG]
  
  def remove(child: DAG): T
  
  def removeAll(toRemove: Iterable[DAG]): DAG = toRemove.foldLeft(this)(_.remove(_))
  
  def print(indent: Int = 0, via: Option[LId] = None, doPrint: String => Any = println): Unit
  
  /**
   * Returns an iterator that does a post-order traversal of this tree
   */
  def iterator: Iterator[DAG] = postOrder
  
  /**
   * Returns an iterator that does a post-order traversal of this tree; that is, 
   * this node's children (dependencies/inputs) are visited before this node.  
   */
  def postOrder: Iterator[DAG] = childIterator(_.postOrder) ++ Iterator.single(this)
  
  /**
   * Returns an iterator that does a pre-order traversal of this tree; that is, 
   * this node is visited before its children (dependencies/inputs).  
   */
  def preOrder: Iterator[DAG] = Iterator.single(this) ++ childIterator(_.preOrder)
  
  protected def childIterator(iterationStrategy: DAG => Iterator[DAG]): Iterator[DAG]

  def chunks: Stream[Set[DAG]] = {
    val myLeaves = this.leaves
    
    myLeaves #:: {
      if(isLeaf) { Stream.empty } 
      else { this.removeAll(myLeaves).chunks }
    }
  }
}

object DAG {

  def apply(tool: Tool): ToolNode = ToolNode(tool)
  
  def apply(store: Store): StoreNode = StoreNode(store)
  
  final case class StoreNode(store: Store, producedByTools: Set[ToolNode] = Set.empty) extends DAG {
    override val id: LId = store.id
    
    override type T = StoreNode
    
    def producedBy(producer: ToolNode): StoreNode = copy(producedByTools = this.producedByTools + producer)
    
    override def isLeaf: Boolean = producedByTools.isEmpty
    
    override def leaves: Set[DAG] = {
      if(isLeaf) { Set(this) }
      else { producedByTools.flatMap(_.leaves) }
    }
    
    override protected def childIterator(iterationStrategy: DAG => Iterator[DAG]): Iterator[DAG] = {
      producedByTools.iterator.flatMap(iterationStrategy)
    }
    
    def withProducers(newProducers: Set[ToolNode]): StoreNode = copy(producedByTools = newProducers)
    
    override def remove(child: DAG): StoreNode = {
      
      if((child eq this) || isLeaf) { this }
      else {
        val newProducers = child match {
          case s: StoreNode => producedByTools
          case t: ToolNode => (producedByTools - t)
        }
        
        withProducers(newProducers.map(_.remove(child)))
      }
    }
    
    override def print(indent: Int = 0, via: Option[LId] = None, doPrint: String => Any = println): Unit = {
      val indentString = s"${" " * indent}^--${via.map(v => s"(${v.name})").getOrElse("")}-"

      doPrint(s"$indentString Store(${store.id})")

      producedByTools.foreach(_.print(indent + 2))
    }
  }

  final case class ToolNode(tool: Tool, inputs: Map[LId, StoreNode] = Map.empty) extends DAG { self =>
    override val id: LId = tool.id
    
    override type T = ToolNode
    
    override def isLeaf: Boolean = inputs.isEmpty
    
    override def leaves: Set[DAG] = {
      if(isLeaf) { Set(this) }
      else { inputs.values.flatMap(_.leaves).toSet }
    }
    
    override protected def childIterator(iterationStrategy: DAG => Iterator[DAG]): Iterator[DAG] = {
      inputs.values.iterator.flatMap(iterationStrategy)
    }
    
    def withInputs(newInputs: Map[LId, StoreNode]): ToolNode = copy(inputs = newInputs)
    
    override def remove(child: DAG): ToolNode = {
      
      if((child eq this) || isLeaf) { this }
      else {
        val newInputs = child match {
          case s: StoreNode => this.inputs.filter { case (id, storeNode) => storeNode != s }
          case t: ToolNode => this.inputs 
        }
        
        withInputs(newInputs.mapValues(_.remove(child)))
      }
    }
    
    override def print(indent: Int = 0, via: Option[LId] = None, doPrint: String => Any = println): Unit = {
      val indentString = s"${" " * indent}^--${via.map(v => s"(${v.name})").getOrElse("")}-"

      doPrint(s"$indentString Tool(${tool.id})")

      inputs.foreach { case (inputId, inputStore) => inputStore.print(indent + 2, via = Some(inputId)) }
    }
    
    def gets(inputId: LId): NamedReceiver = NamedReceiver(this, inputId)

    def apply(outputId: LId): StoreNode = output(outputId)
    
    def output(outputId: LId): StoreNode = {
      require(tool.outputs.contains(outputId))
      
      val storeNode = tool.outputs.get(outputId).map(StoreNode(_)).get //TODO

      storeNode.producedBy(this)
    }
    
    def output(): StoreNode = {
      require(tool.outputs.size == 1)
      
      val outputId = tool.outputs.keys.head
      
      output(outputId)
    }
    
    def take(input: StoreNode): ToolNode = {
      val inputStores = this.tool.inputs
      
      require(inputStores.size == 1)
      
      gets(inputStores.keys.head).from(input) 
    }
  }

  final case class NamedReceiver(receiver: ToolNode, inputId: LId) {
    def from(store: StoreNode): ToolNode = receiver.copy(inputs = receiver.inputs + (inputId -> store))
  }
  
  object Foo {
    val vcf = Paths.get("foo")
    val pca = Paths.get("bar")
    val kkc = KlustaKwikKonfig(Paths.get("/"), "foo")
    
    val aiPipeline = AncestryInferencePipeline(vcf, pca, kkc)
    
    /*
     * val genotypesTool: Tool = CoreTool.CheckPreExistingVcfFile(vcfFile)

  val pcaWeightsTool: Tool = CoreTool.CheckPreExistingPcaWeightsFile(pcaWeightsFile)

  val pcaProjectionTool: Tool = CoreTool.ProjectPca(vcfFile, pcaWeightsFile, klustaConfig)

  val sampleClusteringTool: Tool = CoreTool.ClusteringSamplesByFeatures(klustaConfig)
     */
    
    val vcfToolNode = DAG(aiPipeline.genotypesTool)
    val pcaToolNode = DAG(aiPipeline.pcaWeightsTool)
    
    val pcaProjectToolNode = DAG(aiPipeline.pcaProjectionTool)
    
    val pcaProjectNode = pcaProjectToolNode.gets(LId.newAnonId).from(pcaToolNode.output()).gets(LId.newAnonId).from(vcfToolNode.output())
    
    val dag: ToolNode = DAG(aiPipeline.sampleClusteringTool).take(pcaProjectNode.output())
  }
}