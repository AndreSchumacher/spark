/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.parquet

import java.io.File

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import org.apache.spark.util.Utils
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.hive.TestHive._
import org.apache.spark.sql.catalyst.types.{StringType, IntegerType}
import org.apache.spark.sql.SchemaRDD

class HiveParquetSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val dirname = Utils.createTempDir()

  var testRDD: SchemaRDD = null

  override def beforeAll() {
    // write test data
    ParquetTestData.writeFile
    testRDD = parquetFile(ParquetTestData.testDir.toString)
    testRDD.registerAsTable("testsource")
  }

  override def afterAll() {
    Utils.deleteRecursively(ParquetTestData.testDir)
    Utils.deleteRecursively(dirname)
    reset() // drop all tables that were registered as part of the tests
  }

  // in case tests are failing we delete before and after each test
  override def beforeEach() {
    Utils.deleteRecursively(dirname)
  }

  override def afterEach() {
    Utils.deleteRecursively(dirname)
  }

  test("SELECT on Parquet table") {
    val rdd = sql("SELECT * FROM testsource").collect()
    assert(rdd != null)
    assert(rdd.forall(_.size == 6))
  }

  test("Simple column projection + filter on Parquet table") {
    val rdd = sql("SELECT myboolean, mylong FROM testsource WHERE myboolean=true").collect()
    assert(rdd.size === 5, "Filter returned incorrect number of rows")
    assert(rdd.forall(_.getBoolean(0)), "Filter returned incorrect Boolean field value")
  }

  test("Converting Hive to Parquet Table via saveAsParquetFile") {
    sql("SELECT * FROM src").saveAsParquetFile(dirname.getAbsolutePath)
    parquetFile(dirname.getAbsolutePath).registerAsTable("ptable")
    val rddOne = sql("SELECT * FROM src").collect().sortBy(_.getInt(0))
    val rddTwo = sql("SELECT * from ptable").collect().sortBy(_.getInt(0))
    compareRDDs(rddOne, rddTwo, "src (Hive)", Seq("key:Int", "value:String"))
  }

  test("INSERT OVERWRITE TABLE Parquet table") {
    sql("SELECT * FROM testsource").saveAsParquetFile(dirname.getAbsolutePath)
    parquetFile(dirname.getAbsolutePath).registerAsTable("ptable")
    // let's do three overwrites for good measure
    sql("INSERT OVERWRITE TABLE ptable SELECT * FROM testsource").collect()
    sql("INSERT OVERWRITE TABLE ptable SELECT * FROM testsource").collect()
    sql("INSERT OVERWRITE TABLE ptable SELECT * FROM testsource").collect()
    val rddCopy = sql("SELECT * FROM ptable").collect()
    val rddOrig = sql("SELECT * FROM testsource").collect()
    assert(rddCopy.size === rddOrig.size, "INSERT OVERWRITE changed size of table??")
    compareRDDs(rddOrig, rddCopy, "testsource", ParquetTestData.testSchemaFieldNames)
  }

  test("CREATE TABLE of Parquet table") {
    createParquetFile(dirname.getAbsolutePath, ("key", IntegerType), ("value", StringType))
      .registerAsTable("tmp")
    val rddCopy =
      sql("INSERT INTO TABLE tmp SELECT * FROM src")
      .collect()
      .sortBy[Int](_.apply(0) match {
        case x: Int => x
        case _ => 0
      })
    val rddOrig = sql("SELECT * FROM src")
      .collect()
      .sortBy(_.getInt(0))
    compareRDDs(rddOrig, rddCopy, "src (Hive)", Seq("key:Int", "value:String"))
  }

  test("Appending to Parquet table") {
    createParquetFile(dirname.getAbsolutePath, ("key", IntegerType), ("value", StringType))
      .registerAsTable("tmpnew")
    sql("INSERT INTO TABLE tmpnew SELECT * FROM src").collect()
    sql("INSERT INTO TABLE tmpnew SELECT * FROM src").collect()
    sql("INSERT INTO TABLE tmpnew SELECT * FROM src").collect()
    val rddCopies = sql("SELECT * FROM tmpnew").collect()
    val rddOrig = sql("SELECT * FROM src").collect()
    assert(rddCopies.size === 3 * rddOrig.size, "number of copied rows via INSERT INTO did not match correct number")
  }

  test("Appending to and then overwriting Parquet table") {
    createParquetFile(dirname.getAbsolutePath, ("key", IntegerType), ("value", StringType))
      .registerAsTable("tmp")
    sql("INSERT INTO TABLE tmp SELECT * FROM src").collect()
    sql("INSERT INTO TABLE tmp SELECT * FROM src").collect()
    sql("INSERT OVERWRITE TABLE tmp SELECT * FROM src").collect()
    val rddCopies = sql("SELECT * FROM tmp").collect()
    val rddOrig = sql("SELECT * FROM src").collect()
    assert(rddCopies.size === rddOrig.size, "INSERT OVERWRITE did not actually overwrite")
  }

  private def compareRDDs(rddOne: Array[Row], rddTwo: Array[Row], tableName: String, fieldNames: Seq[String]) {
    var counter = 0
    (rddOne, rddTwo).zipped.foreach {
      (a,b) => (a,b).zipped.toArray.zipWithIndex.foreach {
        case ((value_1, value_2), index) =>
          assert(value_1 === value_2, s"table $tableName row $counter field ${fieldNames(index)} don't match")
      }
    counter = counter + 1
    }
  }
}
