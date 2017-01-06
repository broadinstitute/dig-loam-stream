package loamstream.util

import org.scalatest.FunSuite
import java.nio.file.Paths
import loamstream.TestHelpers

/**
 * @author clint
 * Jan 6, 2017
 */
final class LoamToScalaConverterTest extends FunSuite {
  import LoamToScalaConverter._
  
  import TestHelpers.path
  
  test("listLoamFiles - non-recursive") {
    val root = path("src/main/loam/qc")
    
    val files = listLoamFiles(root, recursive = false)
    
    val expected = Set(
      path("src/main/loam/qc/binaries_cloud.loam"),
      path("src/main/loam/qc/binaries.loam"),
      path("src/main/loam/qc/camp_cloud.loam"),
      path("src/main/loam/qc/camp.loam"),
      path("src/main/loam/qc/kinship.loam"),
      path("src/main/loam/qc/pca_cloud.loam"),
      path("src/main/loam/qc/pca_projection_cloud.loam"),
      path("src/main/loam/qc/pca.loam"))
      
    assert(files.toSet === expected) 
  }
  
  test("listLoamFiles - recursive") {
    val root = path("src/main/loam/qc")
    
    val files = listLoamFiles(root, recursive = true)
    
    val expected = Set(
      path("src/main/loam/qc/binaries_cloud.loam"),
      path("src/main/loam/qc/binaries.loam"),
      path("src/main/loam/qc/camp_cloud.loam"),
      path("src/main/loam/qc/camp.loam"),
      path("src/main/loam/qc/kinship.loam"),
      path("src/main/loam/qc/pca_cloud.loam"),
      path("src/main/loam/qc/pca_projection_cloud.loam"),
      path("src/main/loam/qc/pca.loam"),
      path("src/main/loam/qc/integration/binaries_cloud.loam"),
      path("src/main/loam/qc/integration/binaries.loam"),
      path("src/main/loam/qc/integration/camp_cloud.loam"),
      path("src/main/loam/qc/integration/kinship.loam"),
      path("src/main/loam/qc/integration/pca.loam"))
      
    assert(files.toSet === expected) 
  }
}
