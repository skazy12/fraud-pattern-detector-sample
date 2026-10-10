package frauddetector

import org.scalatest.funsuite.AnyFunSuite
import java.io.File

class SampleFixtureSpec extends AnyFunSuite {
  test("Sample controlled fixture directory and files exist") {
    val sampleDir = new File("../data/sample")
    assert(sampleDir.exists() && sampleDir.isDirectory, "data/sample directory must exist")

    val customersFile = new File("../data/sample/customers.csv")
    val devicesFile = new File("../data/sample/devices.csv")
    val transactionsFile = new File("../data/sample/transactions.csv")
    val manifestFile = new File("../data/sample/generator_manifest.json")
    val expectedFile = new File("../data/sample/scenario_expected.csv")

    assert(customersFile.exists(), "customers.csv must exist in data/sample/")
    assert(devicesFile.exists(), "devices.csv must exist in data/sample/")
    assert(transactionsFile.exists(), "transactions.csv must exist in data/sample/")
    assert(manifestFile.exists(), "generator_manifest.json must exist in data/sample/")
    assert(expectedFile.exists(), "scenario_expected.csv must exist in data/sample/")
  }

  test("Spark can read sample CSV files cleanly") {
    val spark = Main.createSparkSession()
    try {
      val customersDF = spark.read.option("header", "true").csv("../data/sample/customers.csv")
      val devicesDF = spark.read.option("header", "true").csv("../data/sample/devices.csv")
      val transactionsDF = spark.read.option("header", "true").csv("../data/sample/transactions.csv")

      assert(customersDF.count() == 5, "customers.csv should contain 5 records")
      assert(devicesDF.count() == 5, "devices.csv should contain 5 records")
      assert(transactionsDF.count() == 19, "transactions.csv should contain 19 records")
    } finally {
      spark.stop()
    }
  }
}
