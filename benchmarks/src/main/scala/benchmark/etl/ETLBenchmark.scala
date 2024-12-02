/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package benchmark

trait ETLConf extends BenchmarkConf {
  protected def format: Option[String]
  def scaleInGB: Int
  def userDefinedDbName: Option[String]
  def formatName: String = format.getOrElse {
    throw new IllegalArgumentException("format must be specified")
  }
  def dbName: String = userDefinedDbName.getOrElse(s"etl_sf${scaleInGB}_${formatName}")
  def dbLocation: String = dbLocation(dbName)
  def customWriteMode: Option[String]
  def writeMode: String = customWriteMode.getOrElse("copy-on-write")
}

case class ETLBenchmarkConf(
     protected val format: Option[String] = None,
     scaleInGB: Int = 0,
     userDefinedDbName: Option[String] = None,
     iterations: Int = 3,
     benchmarkPath: Option[String] = None,
     sourcePath: Option[String] = None,
     customWriteMode: Option[String] = None) extends ETLConf

object ETLBenchmarkConf {
  import scopt.OParser
  private val builder = OParser.builder[ETLBenchmarkConf]
  private val argParser = {
    import builder._
    OParser.sequence(
      programName("ETL Benchmark"),
      opt[String]("format")
        .required()
        .action((x, c) => c.copy(format = Some(x)))
        .text("Spark's short name for the file format to use"),
      opt[String]("scale-in-gb")
        .required()
        .valueName("<scale of benchmark in GBs>")
        .action((x, c) => c.copy(scaleInGB = x.toInt))
        .text("Scale factor of the ETL benchmark"),
      opt[String]("benchmark-path")
        .required()
        .valueName("<cloud storage path>")
        .action((x, c) => c.copy(benchmarkPath = Some(x)))
        .text("Cloud path to be used for creating table and generating reports"),
      opt[String]("source-path")
        .optional()
        .valueName("<path to the TPC-DS raw input data>")
        .action((x, c) => c.copy(sourcePath = Some(x)))
        .text("The location of the TPC-DS raw input data"),
      opt[String]("iterations")
        .optional()
        .valueName("<number of iterations>")
        .action((x, c) => c.copy(iterations = x.toInt))
        .text("Number of times to run the queries"),
      opt[String]("db-name")
        .optional()
        .valueName("<database name>")
        .action((x, c) => c.copy(userDefinedDbName = Some(x)))
        .text("Name of the target database to create with ETL Prep tables in necessary format"),
      opt[String]("write_mode")
        .optional()
        .valueName("<copy-on-write or merge-on-read>")
        .action((x, c) => c.copy(customWriteMode = Some(x)))
        .text("Strategy used for writing table changes. `copy-on-write` [default] or `merge-on-read`"),
    )
  }

  def parse(args: Array[String]): Option[ETLBenchmarkConf] = {
    OParser.parse(argParser, args, ETLBenchmarkConf())
  }
}

class ETLBenchmark(conf: ETLBenchmarkConf) extends Benchmark(conf) {
  val dbName = conf.dbName
  val dbLocation = conf.dbLocation(dbName, suffix=benchmarkId.replace("-", "_"))
  val sourceFormat = "parquet"
  val formatName = conf.formatName
  val writeMode = conf.writeMode
  val tblProperties = formatName match {
    case "iceberg" =>
      s"""TBLPROPERTIES ('format-version'='2',
                       'write.delete.mode'='${writeMode}',
                       'write.update.mode'='${writeMode}',
                       'write.merge.mode'='${writeMode}')"""
    case "hudi" =>
      // NOTE: This is only used to create single (denormalized) store_sales table;
      //       as such we're reusing primary key we're generally using for store_sales in TPC-DS benchmarks
      // '${if (writeMode == "copy-on-write") "cow" else "mor"}'
      s"""TBLPROPERTIES (
         |  type = 'mor',
         |  primaryKey = 'ss_item_sk,ss_ticket_number',
         |  preCombineField = 'ss_sold_time_sk',
         |  'hoodie.table.name' = 'store_sales_denorm_${formatName}',
         |  'hoodie.table.partition.fields' = 'ss_sold_date_sk',
         |  'hoodie.parquet.compression.codec' = 'snappy',
         |  'hoodie.sql.bulk.insert.enable' = 'true',
         |  'hoodie.table.keygenerator.class' = 'org.apache.hudi.keygen.ComplexKeyGenerator',
         |  'hoodie.datasource.write.hive_style_partitioning' = 'true',
         |  'hoodie.sql.insert.mode'= 'non-strict',
         |  'hoodie.combine.before.insert' = 'false',
         |  'hoodie.metadata.enable' = 'true',
         |  'hoodie.index.type' = 'RECORD_INDEX',
         |  'hoodie.metadata.record.index.enable' = 'true',
         |  'hoodie.metadata.record.index.min.filegroup.count' = 100,
         |  'hoodie.metadata.index.column.stats.enable' = 'true',
         |  'hoodie.metadata.index.column.stats.max.columns.to.index' = '37'
         |)""".stripMargin

    case "delta" => ""
  }
  //   Add below configs to table props to run with RLI or colstats
  //  'hoodie.metadata.record.index.enable' = 'true',
  //  'hoodie.metadata.record.index.min.filegroup.count' = '1000',
  //  'hoodie.metadata.record.index.max.filegroup.count' = '1000',
  //  'hoodie.enable.data.skipping' = 'true',
  //  'hoodie.datasource.write.row.writer.enable' = 'false'
  require(conf.scaleInGB > 0)
  require(Seq(1, 1000, 3000).contains(conf.scaleInGB), "")
  val sourceLocation = conf.sourcePath.getOrElse {
      s"s3://your-default-bucket/path-to-parquet/etl_sf${conf.scaleInGB}_parquet/"
    }
  val extraConfs: Map[String, String] = Map(
    "spark.sql.broadcastTimeout" -> "7200",
    "spark.sql.crossJoin.enabled" -> "true"
  )

  val etlQueries = new ETLQueries(dbLocation, formatName, sourceLocation, sourceFormat, tblProperties)
  val writeQueries: Map[String, String] = etlQueries.writeQueries
  val readQueries: Map[String, String] = etlQueries.readQueries

  def runInternal(): Unit = {
    for ((k, v) <- extraConfs) spark.conf.set(k, v)
    spark.sparkContext.setLogLevel("INFO")
    log("All configs:\n\t" + spark.conf.getAll.toSeq.sortBy(_._1).mkString("\n\t"))

    runQuery(s"DROP DATABASE IF EXISTS ${dbName} CASCADE", s"etl0.1-drop-database")
    runQuery(s"CREATE DATABASE IF NOT EXISTS ${dbName}", s"etl0.2-create-database")
    runQuery(s"USE $dbName", s"etl0.3-use-database")
    runQuery(s"DROP TABLE IF EXISTS store_sales_denorm_${formatName}", s"etl0.4-drop-table")
    runQuery(s"SET hoodie.metadata.enable=true")
    runQuery(s"SET hoodie.metadata.index.column.stats.enable=true")
    runQuery(s"SET hoodie.metadata.record.index.enable=true")
    runQuery(s"SET hoodie.enable.data.skipping=true")
    runQuery(s"SET hoodie.metadata.index.column.stats.max.columns.to.index=37")
    // To just run limited ETL's
    // writeQueries.filter { case (name: String, sql: String) => Seq("etl1-createTable").contains(name) }.toSeq.sortBy(_._1)

    writeQueries.toSeq.sortBy(_._1).foreach { case (name, sql) =>
      runQuery(sql, iteration = Some(1), queryName = name)
      // Print table stats
      if (conf.formatName == "iceberg") {
        runQuery(s"SELECT * FROM spark_catalog.${dbName}.store_sales_denorm_${formatName}.snapshots",
          printRows = true, queryName = s"${name}-file-stats")
      } else if (conf.formatName == "delta") {
        runQuery(s"DESCRIBE HISTORY store_sales_denorm_${formatName}",
          printRows = true, queryName = s"${name}-file-stats")
      }
      // To run with limited queries
      // .filter { case (name: String, sql: String) => Seq("q3", "q6").contains(name) }
      for (iteration <- 1 to conf.iterations) {
        readQueries.toSeq.sortBy(_._1).foreach { case (name, sql) =>
          runQuery(sql, iteration = Some(iteration), queryName = name)
        }
      }
    }
    val results = getQueryResults().filter(_.name.startsWith("q"))
    if (results.forall(x => x.errorMsg.isEmpty && x.durationMs.nonEmpty) ) {
      val medianDurationSecPerQuery = results.groupBy(_.name).map { case (q, results) =>
        log(s"Queries Completed. Checking size: ${results.length}")
        assert(results.size == conf.iterations)
        val medianMs = results.map(_.durationMs.get).sorted
            .drop(math.floor(conf.iterations / 2.0).toInt).head
        (q, medianMs / 1000.0)
      }
      val sumOfMedians = medianDurationSecPerQuery.map(_._2).sum
      reportExtraMetric("ETL-result-seconds", sumOfMedians)
    }
  }
}

object ETLBenchmark {
  def main(args: Array[String]): Unit = {
    ETLBenchmarkConf.parse(args).foreach { conf =>
      new ETLBenchmark(conf).run()
    }
  }
}