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
import loamstream.model.execute.JobFilter
import scala.util.control.NonFatal

/**
 * @author clint
 * Nov 10, 2016
 */
trait AppWiring {
  def config: Config = ConfigFactory.empty()
  
  def dao: LoamDao
  
  def executer: Executer
  
  def shutdown(): Unit = ()
  
  private[AppWiring] def makeJobFilter(conf: Conf): JobFilter = {
    if(conf.runEverything()) JobFilter.RunEverything else new DbBackedJobFilter(dao)
  }
}

object AppWiring extends TypesafeConfigHelpers with DrmaaClientHelpers with Loggable {
  
  def forLocal(conf: Conf): AppWiring = new AppWiring with DefaultDb {
    override val executer: Executer = {
      info("Creating executer...")
      
      RxExecuter.defaultWith(makeJobFilter(conf))
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

      import scala.concurrent.duration._
      
      val windowLength = 30.seconds
      
      val executer = RxExecuter(chunkRunner, windowLength, makeJobFilter(conf))(executionContextWithThreadPool)
      
      new TerminableExecuter(executer, shutdownDrmaaClient, schedulerHandle.shutdown, shutdownJobMonitor)
    }
  }
  
  private trait LoadsConfig { self: AppWiring =>
    def cli: Conf
    
    //NB: This needs to be lazy to avoid some init-order problems
    override lazy val config: Config = {
      def defaults: Config = ConfigFactory.load()
      
      cli.conf.toOption match {
        case Some(confFile) => configFromFile(confFile).withFallback(defaults)
        case None           => defaults
      }
    }
  }
  
  private trait DefaultDb { self: AppWiring =>
    override lazy val dao: LoamDao = {
      val dbDescriptor = DbDescriptor(DbType.H2, "jdbc:h2:./.loamstream/db")
      
      val dao = new SlickLoamDao(dbDescriptor)
      
      dao.createTables()
      
      dao
    }
  }
  
  private[apps] class TerminableExecuter(
      private[apps] val delegate: Executer, shutdownHandles: (() => Any)*) extends Executer {
    
    override def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, JobState] = {
      delegate.execute(executable)(timeout)
    }
     
    def shutdown(): Unit = {
      def quietly(f: => Any): Unit = {
        try { f }
        catch { case NonFatal(e) => error("Error shutting down: ", e) }
      }
      
      shutdownHandles.foreach(handle => quietly(handle()))
    }
  }
}