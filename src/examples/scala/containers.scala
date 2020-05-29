object containers extends loamstream.loam.asscala.LoamFile {
  val fileIn = store("fileIn.txt").asInput
  val fileOut1 = store("fileOut1.txt")
  val fileOut2 = store("fileOut2.txt")
  
  drm {
    cmd"cp $fileIn $fileOut1"
  }
  
  drmWith(imageName = "docker://library/ubuntu:18.04") {
    cmd"cp $fileIn $fileOut2"
  }
}