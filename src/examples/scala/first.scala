object first extends loamstream.loam.asscala.LoamFile {
  val genotypesId = "myImportantGenotypes"
  val cmdName = "doThis"
  val input1 = store(path("input1")).asInput
  val output1 = store(path("output1"))
  
  cmd"$cmdName -id $genotypesId -in $input1 -out $output1"
}