package tools

import tools.LineCommand.lineTokenSep

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 3/15/16.
  */
object LineCommand {
  val lineTokenSep = " "
}

trait LineCommand {

  def name: String

  override def toString: String = name

  trait Param {
    def key: String

    def tokens: Seq[String]
  }

  object ValueParam {

    trait Builder[Value] {
      def apply(value: Value): ValueParam[Value]
    }

  }

  trait ValueParam[Value] extends Param {
    def value: Value
  }

  case class DashSwitchParam(key: String) extends Param {
    override def tokens: Seq[String] = Seq("-" + key)
  }

  object UnkeyedValueParam {

    case class Builder[Value](key: String) extends ValueParam.Builder[Value] {
      def apply(value: Value): UnkeyedValueParam[Value] = UnkeyedValueParam[Value](key, value)
    }

  }

  case class UnkeyedValueParam[Value](key: String, value: Value)
    extends ValueParam[Value] {
    override def tokens: Seq[String] = Seq(value.toString)
  }

  object KeyedValueParam {

    case class Builder[Value](key: String) extends ValueParam.Builder[Value] {
      def apply(value: Value): KeyedValueParam[Value] = KeyedValueParam[Value](key, value)
    }

  }

  case class KeyedValueParam[Value](key: String, value: Value)
    extends ValueParam[Value] {
    override def tokens: Seq[String] = Seq(key, value.toString)
  }

  object DashValueParam {

    case class Builder[Value](key: String) extends ValueParam.Builder[Value] {
      def apply(value: Value): DashValueParam[Value] = DashValueParam[Value](key, value)
    }

  }

  case class DashValueParam[Value](key: String, value: Value)
    extends ValueParam[Value] {
    override def tokens: Seq[String] = Seq("-" + key, value.toString)
  }

  def +(param: Param): CommandLine = CommandLine(this, Seq(param))

  case class CommandLine(command: LineCommand, params: Seq[Param]) {
    def +(param: Param): CommandLine = copy(params = params :+ param)

    def commandLine: String = command + lineTokenSep + params.flatMap(_.tokens).mkString(lineTokenSep)
  }

}
