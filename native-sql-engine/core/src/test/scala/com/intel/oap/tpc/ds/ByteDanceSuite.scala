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

package com.intel.oap.tpc.ds

import com.intel.oap.tpc.util.TPCRunner
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ByteDanceSuite extends QueryTest with SharedSparkSession {

  private val MAX_DIRECT_MEMORY = "6g"
  private val TPCDS_QUERIES_RESOURCE = "bytedance"

  private var runner: TPCRunner = _

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
        .set("spark.plugins", "com.intel.oap.GazellePlugin")
        .set("spark.sql.codegen.wholeStage", "true")
        .set("spark.sql.sources.useV1SourceList", "")
        .set("spark.oap.sql.columnar.tmp_dir", "/tmp/")
        .set("spark.sql.adaptive.enabled", "false")
        .set("spark.sql.columnar.sort.broadcastJoin", "true")
        .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
        .set("spark.executor.heartbeatInterval", "3600000")
        .set("spark.network.timeout", "3601s")
        .set("spark.oap.sql.columnar.preferColumnar", "true")
        .set("spark.oap.sql.columnar.sortmergejoin", "true")
        .set("spark.sql.columnar.codegen.hashAggregate", "false")
        .set("spark.sql.columnar.sort", "true")
        .set("spark.sql.columnar.window", "true")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .set("spark.unsafe.exceptionOnMemoryLeak", "false")
        .set("spark.network.io.preferDirectBufs", "false")
        .set("spark.sql.sources.useV1SourceList", "arrow,parquet")
        .set("spark.sql.autoBroadcastJoinThreshold", "-1")
        .set("spark.oap.sql.columnar.sortmergejoin.lazyread", "true")
        .set("spark.oap.sql.columnar.autorelease", "false")
        .set("spark.master", "local[1]")
    return conf
  }


  override def beforeAll(): Unit = {
    super.beforeAll()
    LogManager.getRootLogger.setLevel(Level.WARN)
    spark.catalog.createTable("tbl_8675", "hdfs://10.1.2.206:9000/bytedance_tables_10k_rows_updated/tbl_8675.parquet", "arrow")
    spark.catalog.createTable("tbl_9551", "hdfs://10.1.2.206:9000/bytedance_tables_10k_rows_updated/tbl_9551.parquet", "arrow")
    spark.catalog.createTable("tbl_8538", "hdfs://10.1.2.206:9000/bytedance_tables_10k_rows_updated/tbl_8538.parquet", "arrow")
    spark.catalog.createTable("tbl_7528", "hdfs://10.1.2.206:9000/bytedance_tables_10k_rows_updated/tbl_7528.parquet", "arrow")
    runner = new TPCRunner(spark, TPCDS_QUERIES_RESOURCE)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }



  test("query") {
    runner.runTPCQuery("q5", 1, true)
  }

  test("window function with non-decimal input") {
    val df = spark.sql("SELECT i_item_sk, i_class_id, SUM(i_category_id)" +
            " OVER (PARTITION BY i_class_id) FROM item LIMIT 1000")
    df.explain()
    df.show()
  }



}


