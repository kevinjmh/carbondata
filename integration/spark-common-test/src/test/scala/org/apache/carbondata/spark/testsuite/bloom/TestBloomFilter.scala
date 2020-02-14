package org.apache.carbondata.spark.testsuite.bloom

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class TestBloomFilter extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll{

  val normalTable = "carbon_normal"
  val bloomTable = "carbon_bloom"

  test("include query on bloom column") {
    sql(
      s"""
         | CREATE TABLE $bloomTable(
         | id int,
         | name STRING
         | )
         | STORED AS carbondata TBLPROPERTIES ('BLOOM_INCLUDE'='id,name')
       """.stripMargin)
    sql(s"insert into $bloomTable values(1,'a',1),(2,'b',2)")
    sql(
      s"""
         | SELECT *
         | FROM $bloomTable WHERE name = 'a'
       """.stripMargin).show()
  }

  test("test bloom on all basic data types") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLED_BLOOM_BLOCKLET, "false");
    queryCheck()

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLED_BLOOM_BLOCKLET, "true");
    sql(s"alter table $bloomTable set tblproperties('CACHE_LEVEL'='BLOCKLET')")
    queryCheck()

    sql(s"alter table $bloomTable set tblproperties('CACHE_LEVEL'='BLOCK')")
    queryCheck()
  }

  def queryCheck() {
    // check simply query
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE booleanField = true"),
      sql(s"SELECT * FROM $normalTable WHERE booleanField = true"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE shortField = 3"),
      sql(s"SELECT * FROM $normalTable WHERE shortField = 3"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE intField = 14"),
      sql(s"SELECT * FROM $normalTable WHERE intField = 14"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE bigintField = 100"),
      sql(s"SELECT * FROM $normalTable WHERE bigintField = 100"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE doubleField = 43.4"),
      sql(s"SELECT * FROM $normalTable WHERE doubleField = 43.4"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE stringField = 'spark'"),
      sql(s"SELECT * FROM $normalTable WHERE stringField = 'spark'"))
    checkAnswer(
      sql(s"SELECT * FROM $bloomTable WHERE timestampField = '2015-7-26 12:01:06'"),
      sql(s"SELECT * FROM $normalTable WHERE timestampField = '2015-7-26 12:01:06'"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE decimalField = 23.23"),
      sql(s"SELECT * FROM $normalTable WHERE decimalField = 23.23"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE dateField = '2015-4-23'"),
      sql(s"SELECT * FROM $normalTable WHERE dateField = '2015-4-23'"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE charField = 'ccc'"),
      sql(s"SELECT * FROM $normalTable WHERE charField = 'ccc'"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE floatField = 2.5"),
      sql(s"SELECT * FROM $normalTable WHERE floatField = 2.5"))

    // check query using null
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE booleanField is null"),
      sql(s"SELECT * FROM $normalTable WHERE booleanField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE shortField is null"),
      sql(s"SELECT * FROM $normalTable WHERE shortField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE intField is null"),
      sql(s"SELECT * FROM $normalTable WHERE intField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE bigintField is null"),
      sql(s"SELECT * FROM $normalTable WHERE bigintField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE doubleField is null"),
      sql(s"SELECT * FROM $normalTable WHERE doubleField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE stringField is null"),
      sql(s"SELECT * FROM $normalTable WHERE stringField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE timestampField is null"),
      sql(s"SELECT * FROM $normalTable WHERE timestampField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE decimalField is null"),
      sql(s"SELECT * FROM $normalTable WHERE decimalField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE dateField is null"),
      sql(s"SELECT * FROM $normalTable WHERE dateField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE charField is null"),
      sql(s"SELECT * FROM $normalTable WHERE charField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE floatField is null"),
      sql(s"SELECT * FROM $normalTable WHERE floatField is null"))

    // check default `NullValue` of measure does not affect result
    // Note: Test data has row contains NULL for each column but no corresponding `NullValue`,
    // so we should get 0 row if query uses the `NullValue`
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE booleanField = false"),
      sql(s"SELECT * FROM $normalTable WHERE booleanField = false"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE shortField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE shortField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE intField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE intField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE bigintField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE bigintField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE doubleField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE doubleField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE decimalField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE decimalField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomTable WHERE floatField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE floatField = 0"))
  }

  override protected def beforeAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomTable")

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    val columnNames = "booleanField,shortField,intField,bigintField,doubleField,stringField," +
      "decimalField,charField,floatField"

    sql(
      s"""
         | CREATE TABLE $bloomTable(
         |    booleanField boolean,
         |    shortField smallint,
         |    intField int,
         |    bigintField bigint,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField string,
         |    floatField float
         | )
         | STORED AS carbondata
         | tblproperties ('bloom_include'='$columnNames')
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE $normalTable(
         |    booleanField boolean,
         |    shortField smallint,
         |    intField int,
         |    bigintField bigint,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField string,
         |    floatField float
         | )
         | STORED AS carbondata
       """.stripMargin)

    (1 to 2).foreach { i =>
      sql(
        s"""
           | INSERT INTO TABLE $bloomTable
           | VALUES(true,1,10,100,48.4,'spark','2015-4-23 12:01:01',1.23,'2015-4-23','aaa',2.5),
           | (true,1,11,100,44.4,'flink','2015-5-23 12:01:03',23.23,'2015-5-23','ccc',2.15),
           | (true,3,14,160,43.4,'hive','2015-7-26 12:01:06',3454.32,'2015-7-26','ff',5.5),
           | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
         """.stripMargin)
      sql(
        s"""
           | INSERT INTO TABLE $normalTable
           | VALUES(true,1,10,100,48.4,'spark','2015-4-23 12:01:01',1.23,'2015-4-23','aaa',2.5),
           | (true,1,11,100,44.4,'flink','2015-5-23 12:01:03',23.23,'2015-5-23','ccc',2.15),
           | (true,3,14,160,43.4,'hive','2015-7-26 12:01:06',3454.32,'2015-7-26','ff',5.5),
           | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
         """.stripMargin)
    }
  }

  override protected def afterAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomTable")
  }
}
