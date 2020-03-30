package loamstream.loam.intake

/**
 * @author clint
 * Jan 3, 2020
 */
sealed trait DataType {
  def name: String 
  
  override def toString: String = name
}

object DataType {
  val String: DataType = ConcreteDataType("STRING")
  val Float: DataType = ConcreteDataType("FLOAT")
  val Int: DataType = ConcreteDataType("INTEGER")
  
  private final case class ConcreteDataType(name: String) extends DataType
  
  import scala.reflect.runtime.universe._
    
  private val byTypeTag: Map[TypeTag[_], DataType] = Map(
    typeTag[Int] -> DataType.Int,
    typeTag[Long] -> DataType.Int,
    typeTag[Double] -> DataType.Float,
    typeTag[Float] -> DataType.Float)
    
  def fromTypeTag[A](tt: TypeTag[A]): DataType = byTypeTag.getOrElse(tt, DataType.String)
}

