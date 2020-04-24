package loamstream.db.slick.migration

import loamstream.db.slick.DbDescriptor
import loamstream.model.execute.Executable
import loamstream.db.slick.OutputDaoOps
import loamstream.db.LoamDao
import loamstream.db.slick.CommandDaoOps
import loamstream.db.slick.OutputRow
import loamstream.db.slick.ExecutionDaoOps
import scala.util.Try
import java.nio.file.Paths
import loamstream.loam.LoamScript
import loamstream.conf.LoamConfig
import com.typesafe.config.ConfigFactory
import loamstream.compiler.LoamProject
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamCompiler
import loamstream.db.slick.SlickLoamDao
import loamstream.model.jobs.StoreRecord
import loamstream.model.jobs.Execution
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import loamstream.db.slick.ExecutionRow
import slick.jdbc.TransactionIsolation
import loamstream.util.TimeUtils
import loamstream.cli.Conf
import loamstream.cli.Intent
import loamstream.apps.AppWiring
import loamstream.util.Loggable

/**
 * @author clint
 * Apr 22, 2020
 */
final class DeleteOldExecutionsAndOutputs(
  override val descriptor: DbDescriptor,
  executable: Executable) extends Migration with LoamDao with OutputDaoOps with ExecutionDaoOps with CommandDaoOps {

  import driver.api._

  override def migrate(): Try[Unit] = {
    def otherOutputsThan(locations: Iterator[String]): Query[tables.Outputs, OutputRow, Seq] = {
      val outputLocs: Set[String] = locations.toSet

      tables.outputs.filterNot(_.locator.inSet(outputLocs))
    }

    def outputLocsFromExecutable: Iterator[String] = {
      executable.allJobs.iterator.flatMap(_.outputs.map(_.location))
    }

    val outputsToDelete = otherOutputsThan(outputLocsFromExecutable)

    val executionsToDelete = tables.executions.filter(_.id.in(outputsToDelete.map(_.executionId)))

    val idsOfExecutionsWithoutOutputs = {
      val join = tables.executions.joinLeft(tables.outputs).on(_.id === _.executionId)

      val withNoOutputs = join.filter { case (_, o) => o.isEmpty }

      withNoOutputs.map { case (e, o) => e.id }
    }

    val executionsWithoutOutputs = tables.executions.filter(_.id.in(idsOfExecutionsWithoutOutputs))

    import Implicits._

    val deleteAction: DBIO[Unit] = for {
      _ <- executionsWithoutOutputs.delete
      _ <- executionsToDelete.delete
    } yield ()

    Try {
      runBlocking(deleteAction.transactionally)
    }
  }
}

object DeleteOldExecutionsAndOutputs extends Loggable {
  def main(args: Array[String]): Unit = {
    val conf = Conf(args)
    
    val intent = Intent.from(conf)
    
    intent match {
      case Left(error) => println(error)
      case Right(intent) => intent match {
        case realRun: Intent.RealRun => doMigration(realRun)
        case x => println(s"Unsupported operation: $x")
      }
    }
    
    def doMigration(realRun: Intent.RealRun): Unit = {
      val descriptor = DbDescriptor.onDiskDefault

      val dao = new SlickLoamDao(descriptor)
      
      val wiring = AppWiring.forRealRun(realRun, dao)

      try {
        val loamEngine = wiring.loamEngine
    
        def loamScripts: Iterable[LoamScript] = {
          val loamFiles = realRun.loams
          val loamScriptsAttempt = loamEngine.scriptsFrom(loamFiles)
          
          require(loamScriptsAttempt.isSuccess, "Could not load loam scripts")
    
          loamScriptsAttempt.get
        }
        
        val project = LoamProject(loamEngine.config, loamScripts)
  
        val success = loamEngine.compile(project).asInstanceOf[LoamCompiler.Result.Success]

        val executable = LoamEngine.toExecutable(success.graph)
        
        val migration = new DeleteOldExecutionsAndOutputs(descriptor, executable)
        
        val attempt = TimeUtils.time("Performing migration...", info(_)) {
          migration.migrate()
        }
        
        info(s"Migration was ${if(attempt.isSuccess) "" else "un"}successful")
        
        if(attempt.isFailure) {
          attempt.get //throw for debugging purposes
        }
      } finally {
        wiring.shutdown()
        dao.shutdown()
      }
    }
  }
}
