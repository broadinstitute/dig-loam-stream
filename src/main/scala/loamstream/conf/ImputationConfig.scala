package loamstream.conf

/**
 * Created on: 5/4/16 
 * @author Kaan Yuksel 
 */
object ImputationConfig {
  val shapeItExecutable = "/humgen/diabetes/users/ryank/software/shapeit/bin/shapeit"
  
  val shapeItBaseDir = "/humgen/diabetes/users/kyuksel/imputation/shapeit_example/"
  val shapeItVcfFile = shapeItBaseDir + "gwas.vcf.gz"
  val shapeItMapFile = shapeItBaseDir + "genetic_map.txt.gz"
  val shapeItHapFile = shapeItBaseDir + "gwas.phased.haps.gz"
  val shapeItSampleFile = shapeItBaseDir + "gwas.phased.sample.gz"
  val shapeItLogFile = shapeItBaseDir + "gwas.phased.log"

  val shapeItNumThreads = 8
}
