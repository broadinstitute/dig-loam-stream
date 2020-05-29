object imputeWorksOnBroadCluster extends loamstream.loam.asscala.LoamFile {
  val kgpDir = path("/humgen/diabetes/users/ryank/internal_qc/1kg_phase3/1000GP_Phase3")
  val softDir = path("/humgen/diabetes/users/ryank/software")
  
  val shapeit = softDir / "shapeit/bin/shapeit"
  val impute2 = softDir / "impute_v2.3.2_x86_64_static/impute2"
  
  val chr = 22
  val start = 1
  val end = 1000000
  
  val homeDir = path("/home/unix/cgilbert")
  val dataDir = homeDir / "imputation"
  val shapeitDataDir = dataDir / "shapeit_example"
  val impute2DataDir = dataDir / "impute2_example"
  
  val outputDir = homeDir / "output"
  
  val data = store(shapeitDataDir / "gwas.vcf.gz").asInput
  val geneticMap = store(shapeitDataDir / "genetic_map.txt.gz").asInput
  val phasedHaps = store(outputDir / "phased.haps.gz")
  val phasedSamples = store(outputDir / "phased.samples.gz")
  val log = store(outputDir / "shapeit.log")
  
  cmd"$shapeit -V $data -M $geneticMap -O $phasedHaps $phasedSamples -L $log --thread 8"
  
  val mapFile = store(impute2DataDir / "example.chr22.map").asInput
  val legend = store(impute2DataDir / "example.chr22.1kG.legend.gz").asInput
  val knownHaps = store(impute2DataDir / "example.chr22.prephasing.impute2_haps.gz").asInput
  val imputed = store(outputDir / "imputed.data.gen")
  
  cmd"""$impute2 -use_prephased_g -m $mapFile -h $phasedHaps -l $legend 
  -known_haps_g $knownHaps -int 20.4e6 20.5e6 -Ne 20000 -o $imputed -verbose -o_gz
  """
}
