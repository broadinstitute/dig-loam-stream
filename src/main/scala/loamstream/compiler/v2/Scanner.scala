package loamstream.compiler.v2

import org.reflections.Reflections
import org.reflections.util.ConfigurationBuilder
import org.reflections.util.ClasspathHelper
import org.reflections.scanners.SubTypesScanner
import scala.collection.JavaConverters
import loamstream.util.code.ObjectId
import loamstream.util.code.ReflectionUtil
import loamstream.compiler.LoamClassLoader
import com.typesafe.config.ConfigFactory
import loamstream.conf.LoamConfig

object Scanner extends App {
  private val scanner: Reflections = new Reflections(
    (new ConfigurationBuilder)
       .setUrls(ClasspathHelper.forPackage("loamstream.compiler.v2"))
       .setScanners(new SubTypesScanner))
  
  def loamFileClasses: Set[Class[_ <: LoamFile]] = {
    import JavaConverters._
    
    scanner.getSubTypesOf(classOf[LoamFile]).asScala.toSet
  }
  
  LoamFile.config = LoamConfig.fromConfig(ConfigFactory.empty).get
  
  val classLoader = new LoamClassLoader(getClass.getClassLoader)
  //val classLoader = getClass.getClassLoader
  
  val classNames = loamFileClasses.map(_.getName /*.dropRight(1)*/ ).toSeq.reverse
  println(s"%%%%% Got (${classNames.size}) class names: $classNames")
  val instances = classNames.map(ReflectionUtil.getObject[LoamFile](classLoader, _))
  println(s"%%%%% Got (${instances.size}) instances: $instances")
  instances.foreach(println)
}
