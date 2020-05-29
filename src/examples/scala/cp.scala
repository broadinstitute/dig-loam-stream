object cp extends loamstream.loam.asscala.LoamFile {
  val fileIn = store("fileIn.txt").asInput
  val fileTmp1 = store
  val fileTmp2 = store
  val fileOut1 = store("fileOut1.txt")
  val fileOut2 = store("fileOut2.txt")
  val fileOut3 = store("fileOut3.txt")
  cmd"cp $fileIn $fileTmp1"
  cmd"cp $fileTmp1 $fileTmp2"
  cmd"cp $fileTmp2 $fileOut1"
  cmd"cp $fileTmp2 $fileOut2"
  cmd"cp $fileTmp2 $fileOut3"
}
