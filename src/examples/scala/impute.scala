object impute extends loamstream.loam.asscala.LoamFile {
  val kgpDir = path("/humgen/diabetes/users/ryank/internal_qc/1kg_phase3/1000GP_Phase3")
  val softDir = path("/humgen/diabetes/users/ryank/software")
  
  val shapeit = softDir / "shapeit/bin/shapeit"
  val impute2 = softDir / "impute_v2.3.2_x86_64_static/impute2"
  
  val chr = 22
  val start = 1
  val end = 1000000
  
  val data = store(s"data.chr${chr}.vcf.gz").asInput
  val geneticMap = store(kgpDir / s"genetic_map_chr${chr}_combined_b37.txt").asInput
  val phasedHaps = store(s"phased.data.${chr}.haps.gz")
  val phasedSample = store(s"phased.data.${chr}.sample.gz")
  val log = store(s"phased.data.${chr}.log")
  
  cmd"$shapeit -V $data -M $geneticMap -O $phasedHaps $phasedSample -L $log --thread 8"
  
  val exampleHaps = store(kgpDir / s"1000GP_Phase3_chr${chr}.hap.gz").asInput
  val exampleLegend = store(kgpDir / s"1000GP_Phase3_chr${chr}.legend.gz").asInput
  val imputed = store(s"imputed.data.chr${chr}bp${start}-${end}.gen")
  
  cmd"""$impute2 -use_prephased_g -m $geneticMap -h $exampleHaps -l $exampleLegend 
  -known_haps_g $phasedHaps -int ${start} ${end} -Ne 20000 -o imputes $imputed -verbose -o_gz
  """
}