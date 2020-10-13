package loamstream.loam.intake.aggregator.util

/**
 * @author clint
 * Oct 9, 2020
 * 
 * Ported from util/assembly_map.py
 */
object AssemblyMap {
  private val Assemblies: Map[String, String] = Map(
    "hg19" -> "GRCh37"
  )
  
  /**
   * True if assembly a matches assembly b. First will translate hgXX assemblies
    to GRChXX assemblies before comparing.
   */
  def matchAssemblies(a: String, b: String): Boolean = {
    def munge(s: String): String = {
      val lowered = s.toLowerCase
      
      if(lowered.startsWith("hg")) Assemblies(lowered) else lowered
    }
    
    val mungedA = munge(a)
    val mungedB = munge(b)
    
    def requireIsValid(s: String): Unit = require(s.startsWith("GRC"), s"Assembly ${s} is invalid!") 
    
    //make sure the assemblies are validly named
    requireIsValid(mungedA)
    requireIsValid(mungedB)

    //compare
    mungedA == mungedB
  }
}

/*
Assemblies = {
    'hg19': 'GRCh37',
}


def match_assemblies(a, b):
    """
    True if assembly a matches assembly b. First will translate hgXX assemblies
    to GRChXX assemblies before comparing.
    """
    if a.lower()[:2] == 'hg':
        a = Assemblies.get(a.lower())
    if b.lower()[:2] == 'hg':
        b = Assemblies.get(b.lower())

    # make sure the assemblies are validly named
    if not a.startswith('GRC'):
        raise Exception('Assembly %s is invalid!' % a)
    if not b.startswith('GRC'):
        raise Exception('Assembly %s is invalid!' % b)

    # compare
    if a == b:
        return True

    # no match
    return False
*/
