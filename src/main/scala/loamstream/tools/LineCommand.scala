package loamstream.tools

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 3/15/16.
  */
object LineCommand {

  //TODO: New name: this trait is, confusingly, if pleasantly symmetically, LineCommand.CommandLine.
  trait CommandLine {
    def tokens: Seq[String]

    def commandLine: String
  }

  val tokenSep = " "
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

  def +(param: Param): CommandLineBuildable = CommandLineBuildable(this, Seq(param))

  case class CommandLineBuildable(command: LineCommand, params: Seq[Param]) extends LineCommand.CommandLine {
    def +(param: Param): CommandLineBuildable = copy(params = params :+ param)

    def tokens: Seq[String] = command.toString +: params.flatMap(_.tokens)

    def commandLine: String = tokens.mkString(LineCommand.tokenSep)
  }

}
