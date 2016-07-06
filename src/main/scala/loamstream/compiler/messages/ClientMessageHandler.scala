package loamstream.compiler.messages

import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.compiler.repo.LoamRepository
import loamstream.compiler.{Issue, LoamCompiler}
import loamstream.loam.LoamToolBox
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.model.execute.ChunkedExecuter
import loamstream.util.{Hit, Loggable, Miss, StringUtils}

import scala.concurrent.ExecutionContext

/** The handler responding to messages sent by a client */
object ClientMessageHandler {

  /** A receiver of messages sent to a client */
  object OutMessageSink {

    /** A receiver of messages that does nothing */
    object NoOp extends OutMessageSink {
      override def send(outMessage: ClientOutMessage): Unit = ()
    }

    /** A receiver of messages that logs messages */
    case class LoggableOutMessageSink(loggable: Loggable) extends OutMessageSink {
      /** Accepts messages and logs them */
      override def send(outMessage: ClientOutMessage): Unit = {
        outMessage match {
          case ErrorOutMessage(message) => loggable.error(message)
          case CompilerIssueMessage(issue) => issue.severity match {
            case Issue.Info => loggable.info(issue.msg)
            case Issue.Warning => loggable.warn(issue.msg)
            case Issue.Error => loggable.error(issue.msg)
          }
          case _ => loggable.info(outMessage.message)
        }
      }
    }

  }

  /** A receiver of messages sent to a client */
  trait OutMessageSink {
    /** Accepts messages to be sent to the client */
    def send(outMessage: ClientOutMessage)
  }

}

/** The handler responding to messages sent by a client */
case class ClientMessageHandler(outMessageSink: OutMessageSink)(implicit executionContext: ExecutionContext) {
  val repo = LoamRepository.defaultRepo
  val compiler = new LoamCompiler(outMessageSink)

  // scalastyle:off cyclomatic.complexity
  /** Handles messages sent in by a client */
  def handleInMessage(inMessage: ClientInMessage): Unit = {
    inMessage match {
      case CompileRequestMessage(code) =>
        outMessageSink.send(ReceiptOutMessage(code))
        compiler.compile(code)
      case RunRequestMessage(code) =>
        outMessageSink.send(ReceiptOutMessage(code))
        val compileResults = compiler.compile(code)
        if (!compileResults.isValid) {
          outMessageSink.send(ErrorOutMessage("Could not compile."))
        } else {
          outMessageSink.send(StatusOutMessage(compileResults.summary))
          val env = compileResults.envOpt.get
          val graph = compileResults.graphOpt.get.withEnv(env)
          val mapping = LoamGraphAstMapper.newMapping(graph)
          val toolBox = LoamToolBox(env)
          val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)
          outMessageSink.send(StatusOutMessage("Now going to execute."))
          val jobResults = ChunkedExecuter.default.execute(executable)
          outMessageSink.send(StatusOutMessage(s"Done executing ${StringUtils.soMany(jobResults.size, "job")}."))
        }
      case LoadRequestMessage(name) =>
        repo.load(name) match {
          case Hit(loadResponseMessage) => outMessageSink.send(loadResponseMessage)
          case Miss(snag) => outMessageSink.send(ErrorOutMessage(s"Could not load $name: ${snag.message}"))
        }
      case ListRequestMessage =>
        outMessageSink.send(ListResponseMessage(repo.list))
      case SaveRequestMessage(name, content) =>
        repo.save(name, content) match {
          case Hit(saveResponseMessage) => outMessageSink.send(saveResponseMessage)
          case Miss(snag) => outMessageSink.send(ErrorOutMessage(s"Could not save $name: ${snag.message}"))
        }
      case _ =>
        outMessageSink.send(ErrorOutMessage(s"Don't know what to do with incoming socket message '$inMessage'."))
    }
  }

  // scalastyle:on cyclomatic.complexity

}
