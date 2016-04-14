package loamstream.tools.klusta

import scala.util.Random

/**
  * LoamStream
  * Created by oliverr on 3/16/2016.
  */
object KlustaKwikMockDataGenerator {
  private val random = new Random

  def generate(nSamples: Int, nPcas: Int, nClusters: Int, random: Random = this.random): Seq[Seq[Double]] = {
    val numberThatGivesEnoughSpaceForClusters = 10.0
    val clusterCenters = Seq.fill(nClusters, nPcas)(numberThatGivesEnoughSpaceForClusters * random.nextDouble())
    
    Seq.fill(nSamples) {
      val iCluster = random.nextInt(nClusters)
      
      clusterCenters(iCluster).map(_ + random.nextGaussian())
    }
  }
}
