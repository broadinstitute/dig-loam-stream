package loamstream.model.jobs.ops

import java.nio.file.Path

import loamstream.loam.ops.TextStoreRecord
import loamstream.loam.ops.mappers.LoamStoreMapper
import loamstream.model.jobs.ops.StoreMapperJob.LineMapper
import loamstream.util.Files

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe.Type
import loamstream.model.execute.{ExecutionEnvironment, LocalSettings}
import loamstream.model.jobs.{Execution, JobStatus, LJob, Output}

/** Job which creates a new store by mapping an existing store */
object StoreMapperJob {

  /** Adapter to get a String => String from a LoamStoreMapper */
  final case class LineMapper(mapper: LoamStoreMapper.Untyped, tpeIn: Type, tpeOut: Type) extends (String => String) {
    override def apply(line: String): String = {
      val record = TextStoreRecord(line)
      
      mapper.mapDynamicallyTyped(record, tpeIn, tpeOut).asInstanceOf[TextStoreRecord].text
    }
  }

}

/** Job which creates a new store by filtering an existing store */
final case class StoreMapperJob(
    inPath: Path, 
    outPath: Path, 
    inType: Type, 
    outType: Type, 
    inputs: Set[LJob],
    outputs: Set[Output], mapper: LoamStoreMapper.Untyped) extends LJob {
  
  //TODO: See if this is always the case
  override def executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.Local
  
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  /** Implementations of this method will do any actual work to be performed by this job */
  override protected def executeSelf(implicit context: ExecutionContext): Future[Execution] = Future {
    Files.mapFile(inPath, outPath)(LineMapper(mapper, inType, outType))
    Execution(executionEnvironment,
              None,
              LocalSettings(),
              JobStatus.Succeeded,
              None, // TODO: Is this right?
              None,
              outputs.map(_.toOutputRecord))
  }
}
