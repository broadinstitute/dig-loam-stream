package tools

import tools.CommandLine.{Command, Param, lineTokenSep}

/**
  * LoamStream
  * Created by oliverr on 3/15/2016.
  */
object CommandLine {

  val lineTokenSep = " "

  trait Command {
    def name: String

  }

  trait Param[Owner <: Command] {
    def key: String

    def tokens: Seq[String]
  }

  object ValueParam {

    trait Builder[Owner <: Command, Value] {
      def apply(value: Value): ValueParam[Owner, Value]
    }

  }

  trait ValueParam[Owner <: Command, Value] extends Param[Owner] {
    def value: Value
  }

  case class DashSwitchParam[Owner <: Command](key: String) extends Param[Owner] {
    override def tokens: Seq[String] = Seq("-" + key)
  }

  object DashValueParam {

    case class Builder[Owner <: Command, Value](key: String) extends ValueParam.Builder[Owner, Value] {
      def apply(value: Value): DashValueParam[Owner, Value] = DashValueParam[Owner, Value](key, value)
    }

  }

  case class DashValueParam[Owner <: Command, Value](key: String, value: Value)
    extends ValueParam[Owner, Value] {
    override def tokens: Seq[String] = Seq("-" + key, value.toString)
  }

}

case class CommandLine[C <: Command](command: String, params: Seq[Param]) {
  def +(param: Param): CommandLine[C] = copy(params = params :+ param)

  def commandLine: String = command + lineTokenSep + params.flatMap(_.tokens).mkString(lineTokenSep)
}
