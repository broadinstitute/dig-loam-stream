package loamstream.apps

import com.typesafe.config.Config
import loamstream.db.slick.DbDescriptor
import loamstream.db.LoamDao
import loamstream.model.execute.ChunkRunner
import loamstream.model.execute.Executer
import loamstream.cli.Conf
import com.typesafe.config.ConfigFactory
import loamstream.db.slick.SlickLoamDao
import loamstream.db.slick.DbType
import loamstream.uger.UgerChunkRunner
import loamstream.conf.UgerConfig
import loamstream.util.Loggable
import loamstream.uger.DrmaaClient
import loamstream.uger.Poller
import loamstream.util.RxSchedulers
import loamstream.uger.JobMonitor
import loamstream.model.execute.RxExecuter
import loamstream.model.execute.Executable
import scala.concurrent.duration.Duration
import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobState
import loamstream.model.execute.DbBackedJobFilter

/**
 * @author clint
 * Nov 10, 2016
 */
trait AppWiring {
  def config: Config = ConfigFactory.empty()
  
  def dao: LoamDao
  
  def executer: Executer
  
  def shutdown(): Unit = ()
}

object AppWiring extends TypesafeConfigHelpers with DrmaaClientHelpers with Loggable {
  
  def forLocal: AppWiring = new AppWiring with DefaultDb {
    override val executer: Executer = {
      info("Creating executer...")
      
      RxExecuter.defaultWith(new DbBackedJobFilter(dao))
    }
  }
  
  def forUger(conf: Conf): AppWiring = new AppWiring with LoadsConfig with DefaultDb {
    override val cli = conf
    
    override def executer: Executer = terminableExecuter
      
    override def shutdown(): Unit = terminableExecuter.shutdown()
    
    private val terminableExecuter: TerminableExecuter = {
      
      debug("Parsing Uger config")
      
      val ugerConfig = UgerConfig.fromConfig(config).get
      
      info("Creating executer...")

      val (drmaaClient, shutdownDrmaaClient) = getDrmaaClient
      
      import loamstream.model.execute.ExecuterHelpers._
      
      val threadPoolSize = 50
      val executionContextWithThreadPool = threadPool(threadPoolSize)

      val pollingFrequencyInHz = 0.1
    
      val poller = Poller.drmaa(drmaaClient)
      
      val (scheduler, schedulerHandle) = RxSchedulers.backedByThreadPool(threadPoolSize)
    
      val jobMonitor = new JobMonitor(scheduler, poller, pollingFrequencyInHz)
    
      val shutdownJobMonitor = () => jobMonitor.stop()
      
      val chunkRunner = UgerChunkRunner(ugerConfig, drmaaClient, jobMonitor, pollingFrequencyInHz)

      val executer = RxExecuter(chunkRunner)(executionContextWithThreadPool)
      
      new TerminableExecuter {
        override def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, JobState] = {
          executer.execute(executable)(timeout)
        }
        
        override def shutdown(): Unit = {
          shutdownDrmaaClient()
          schedulerHandle.shutdown()
          shutdownJobMonitor()
        }
      }  
    }
  }
  
  private trait LoadsConfig { self: AppWiring =>
    def cli: Conf
    
    override val config: Config = {
      val fromFile = cli.conf.toOption match {
        case Some(confFile) => configFromFile(confFile)
        case None           => ConfigFactory.empty
      }

      fromFile.withFallback(ConfigFactory.load())
    }
  }
  
  private trait DefaultDb { self: AppWiring =>
    override val dao: LoamDao = {
      val dbDescriptor = DbDescriptor(DbType.H2, "jdbc:h2:./.loamstream/db")
      
      new SlickLoamDao(dbDescriptor)
    }
  }
  
  private trait TerminableExecuter extends Executer {
    def shutdown(): Unit
  }
}