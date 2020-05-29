object imputeParallelCampChr22 extends loamstream.loam.asscala.LoamFile {
  val kgpDir = path("/humgen/diabetes/users/ryank/data/1kg_phase3/1000GP_Phase3")
  val softwareDir = path("/humgen/diabetes/users/ryank/software")
  
  val shapeit = softwareDir / "shapeit/bin/shapeit"
  val impute2 = softwareDir / "impute_v2.3.2_x86_64_static/impute2"
  
  val nShards = 36
  val offset = 16050074
  val basesPerShard = 1000000
  
  val shapeitDataDir =  path("/humgen/diabetes/users/ryank/data/camp/data_clean/split")
  
  val outputDir = path("/humgen/diabetes/users/kyuksel/imputation/run_dir")
  
  val vcfData = store(shapeitDataDir / "camp.biallelic.chr22.clean.vcf.gz").asInput
  val geneticMap = store(kgpDir / "genetic_map_chr22_combined_b37.txt").asInput
  val phasedHaps = store(outputDir / "phased.haps.gz")
  val phasedSamples = store(outputDir / "phased.samples.gz")
  val log = store(outputDir / "shapeit.log")
  
  cmd"$shapeit -V $vcfData -M $geneticMap -O $phasedHaps $phasedSamples -L $log --thread 16"
  
  val mapFile = store(kgpDir / "genetic_map_chr22_combined_b37.txt").asInput
  val legend = store(kgpDir / "1000GP_Phase3_chr22.legend.gz").asInput
  val knownHaps = store(kgpDir / "1000GP_Phase3_chr22.hap.gz").asInput
  
  for(iShard <- 0 until nShards) {
    val start = offset + iShard*basesPerShard + 1
    val end = start + basesPerShard - 1
  
    val imputed = store(outputDir / s"imputed.data.bp${start}-${end}.gen")
  
    cmd"""$impute2 -use_prephased_g -m $mapFile -h $knownHaps -l $legend
    -known_haps_g $phasedHaps -int $start $end -Ne 20000 -o $imputed -verbose -o_gz
    """
  }
}