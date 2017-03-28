package loamstream.loam

import java.nio.file.{Path, Paths}

import loamstream.util.{DepositBox, ValueBox}
import loamstream.model.execute.ExecutionEnvironment
import java.time.Instant

/** Container for compile time and run time context for a script */
final class LoamScriptContext(val projectContext: LoamProjectContext) {

  val workDirBox: ValueBox[Path] = ValueBox(Paths.get("."))

  def workDir: Path = workDirBox.value

  def setWorkDir(newDir: Path): Path = {
    workDirBox.value = newDir
    newDir
  }

  def changeWorkDir(newDir: Path): Path = workDirBox.mutate(_.resolve(newDir)).value
  
  val executionEnvironmentBox: ValueBox[ExecutionEnvironment] = ValueBox(ExecutionEnvironment.Local)

  def executionEnvironment: ExecutionEnvironment = executionEnvironmentBox.value

  def executionEnvironment_=(newEnv: ExecutionEnvironment): Unit = {
    executionEnvironmentBox.value = newEnv
  }
  
  //TODO
  lazy val executionId: String = s"${Instant.now.toString.replaceAll(":",".")}-${java.util.UUID.randomUUID}"
}

/** Container for compile time and run time context for a script */
object LoamScriptContext {
  def fromDepositedProjectContext(receipt: DepositBox.Receipt): LoamScriptContext =
    new LoamScriptContext(LoamProjectContext.depositBox(receipt))
}
