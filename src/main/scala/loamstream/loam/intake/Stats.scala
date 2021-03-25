package loamstream.loam.intake

import breeze.stats.distributions.Gaussian

/**
 * @author clint
 * 
 * 16 Mar, 2021
 */
object Stats {
  /**
   * Ported from dig-aggregator-intake/processors/variants.py
   */
  def effectiveN(cases: Double, controls: Double): Double = 4.0 / ((1.0 / cases) + (1.0 / controls))
  
  private val threadLocalGaussian: ThreadLocal[Gaussian] = {
    ThreadLocal.withInitial(() => Gaussian(mu = 0, sigma = 1)) 
  }
  
  /**
   * Quantized function.
   * See: https://www.rdocumentation.org/packages/stats/versions/3.5.1/topics/Normal
   * 
   * Ported from dig-aggregator-intake/processors/variants.py
   */
  def qnorm(beta: Double, pvalue: Double): Double = {
    //TODO: Is it safe to cache this?
    val gaussian = threadLocalGaussian.get()
    
    import gaussian.inverseCdf
    import scala.math.abs
    
    if(beta == 0.0) { 1.0 }
    else { -(abs(beta) / inverseCdf(pvalue / 2)) }
  }
}