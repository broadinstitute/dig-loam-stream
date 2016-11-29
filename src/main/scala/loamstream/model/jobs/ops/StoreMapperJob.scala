package loamstream.model.jobs.ops

import java.nio.file.Path

import loamstream.loam.ops.TextStoreRecord
import loamstream.loam.ops.mappers.LoamStoreMapper
import loamstream.model.jobs.ops.StoreMapperJob.LineMapper
import loamstream.model.jobs.{JobState, LJob, Output}
import loamstream.util.Files

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe.Type

/** Job which creates a new store by filtering an existing store */
object StoreMapperJob {

  final case class LineMapper(mapper: LoamStoreMapper.Untyped, tpeIn: Type, tpeOut: Type) extends (String => String) {
    override def apply(line: String): String = {
      val record = TextStoreRecord(line)
      mapper.mapDynamicallyTyped(record, tpeIn, tpeOut).asInstanceOf[TextStoreRecord].text
    }
  }

}

/** Job which creates a new store by filtering an existing store */
final case class StoreMapperJob(inPath: Path, outPath: Path, inType: Type, outType: Type, inputs: Set[LJob],
                                outputs: Set[Output], mapper: LoamStoreMapper.Untyped)
  extends LJob {
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  /** Implementations of this method will do any actual work to be performed by this job */
  override protected def executeSelf(implicit context: ExecutionContext): Future[JobState] = Future {
    Files.mapFile(inPath, outPath)(LineMapper(mapper, inType, outType))
    JobState.Succeeded
  }

}
