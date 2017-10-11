package loamstream.loam

import java.nio.file.Path
import java.nio.file.Paths

import loamstream.util.DepositBox
import loamstream.util.ValueBox
import loamstream.model.execute.Environment

/** Container for compile time and run time context for a script */
final class LoamScriptContext(val projectContext: LoamProjectContext) {

  val workDirBox: ValueBox[Path] = ValueBox(Paths.get("."))

  def workDir: Path = workDirBox.value

  def setWorkDir(newDir: Path): Path = {
    workDirBox.value = newDir
    newDir
  }

  def changeWorkDir(newDir: Path): Path = workDirBox.mutate(_.resolve(newDir)).value
  
  val environmentBox: ValueBox[Environment] = ValueBox(Environment.Local)

  def executionEnvironment: Environment = environmentBox.value

  def executionEnvironment_=(newEnv: Environment): Unit = {
    environmentBox.value = newEnv
  }

  lazy val executionId: String = s"${java.util.UUID.randomUUID}"
}

/** Container for compile time and run time context for a script */
object LoamScriptContext {
  def fromDepositedProjectContext(receipt: DepositBox.Receipt): LoamScriptContext = {
    new LoamScriptContext(LoamProjectContext.depositBox(receipt))
  }
}
