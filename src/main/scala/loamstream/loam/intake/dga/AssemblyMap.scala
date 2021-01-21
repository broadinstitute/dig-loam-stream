package loamstream.loam.intake.dga

/**
 * @author clint
 * Jan 15, 2021
 * 
 * @see processors/util/assembly_map.py
 */
object AssemblyMap {
  val Assemblies: Map[String, String] = Map(
    "hg19" -> "GRCh37"
  )
  
  /**
   * True if assembly a matches assembly b. First will translate hgXX assemblies
   * to GRChXX assemblies before comparing.
   */
  def match_assemblies(a: String, b: String): Boolean = {
    def mapAssemblyIdIfNeeded(assemblyId: String): String = {
      val loweredId = assemblyId.toLowerCase
      
      if(loweredId.startsWith("hg")) {
        require(
            Assemblies.contains(loweredId), 
            s"No mapping found for assembly id '${loweredId}'.  Known mappings are: ${Assemblies.mkString(",")}")
            
        Assemblies(loweredId)
      } 
      else { assemblyId }
    }
    
    def requireValid(assemblyId: String): Unit = {
      require(assemblyId.startsWith("GRC"), s"Assembly '${assemblyId}' is invalid!")
    }
    
    val mappedA = mapAssemblyIdIfNeeded(a)
    val mappedB = mapAssemblyIdIfNeeded(b)
    
    //make sure the assemblies are validly named
    requireValid(mappedA)
    requireValid(mappedB)
    
    //compare
    mappedA == mappedB
  }
}
