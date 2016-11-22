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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.column.statistics.histogram.HistogramStatistics
import org.apache.parquet.filter2.statisticslevel.InRange
import org.apache.parquet.hadoop.{Footer, ParquetFileReader}
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.sources.{And, Filter}
import org.apache.spark.util.SerializableConfiguration


case class RowGroupHistogramInfo(
                                  fileName: String,
                                  var filePath: String,
                                  start: Long, length: Long,
                                  histograms: collection.mutable.HashMap[String,
                                    HistogramStatistics[Long]],
                                  var hosts: Array[String],
                                  partitionValue: InternalRow)

object ParquetHistogramUtil {

  // RowGroupMetaData is cached here
  private var cachedRowGroupMetadata: Seq[RowGroupHistogramInfo] = null

  def isCachedRowGroupMetadata(): Boolean = {
    if ( cachedRowGroupMetadata == null )
      return false
    else
      return true
  }

  def getCachedRowGroupMetadata(): Seq[RowGroupHistogramInfo] = {
    return cachedRowGroupMetadata
  }

  def setCachedRowGroupMetadata(metadata: Seq[RowGroupHistogramInfo]): Unit = {
    ParquetHistogramUtil.cachedRowGroupMetadata = metadata
  }

  def useHistogram(filters: Seq[Filter]): Boolean = {
    if(filters.size == 0 )
      return false
    else
      regex_gt.findAllMatchIn(filters(0).toString).size < 3
  }

  val regex_gt = """GreaterThan\((\w*),(\d*)\)""".r
  val regex_lt = """LessThan\((\w*),(\d*)\)""".r
  val regex_equal = """EqualTo\((\w*),(\d*)\)""".r

  /**
    * Function to get [InRange] object lists by [Filter]
     @param filters
    * @return
    */
  def getFilterObjects(filters: Filter): Seq[InRange] = {
    val planText: String = filters.toString
    val gt = ParquetHistogramUtil.regex_gt
    val lt = ParquetHistogramUtil.regex_lt
    val equal = ParquetHistogramUtil.regex_equal

    // A map where key = column name , value = [low bound, up bound]
    val gt_lt_map = collection.mutable.HashMap.empty[String, (Long, Long)]

    // lower bound
    for (x <- gt.findAllMatchIn(planText)) {
      gt_lt_map += (x.group(1) -> (x.group(2).toLong, Long.MaxValue))
    }

    // upper bound
    for (x <- lt.findAllMatchIn(planText)) {
      val columnName = x.group(1)
      val tmp = gt_lt_map(columnName)
      gt_lt_map += (columnName -> (tmp._1, x.group(2).toLong))
    }

    val gt_lt_inRange = gt_lt_map.map({ k_v =>
      val colName = k_v._1
      val upper_lower = k_v._2

      val inRange = new InRange(colName)
      inRange.setLower(upper_lower._1)
      inRange.setUpper(upper_lower._2)

      inRange
    })

    val equal_inRange = equal.findAllMatchIn(planText).map({ x =>
      val inRange = new InRange(x.group(1))
      inRange.setLower(x.group(2).toLong)
      inRange.setUpper(x.group(2).toLong)
      inRange
    })

    (gt_lt_inRange ++ equal_inRange).toSeq
  }

  /**
    * Function to get a list of Row Group by parquet files
    * @param filesToTouch
    * @param sparkSession
    * @return
    */
  def getRowGroupHistogramInfoSeq(
                       filesToTouch: Seq[FileStatus],
                       sparkSession: SparkSession,
                       partitionValue: InternalRow)
                            : Seq[RowGroupHistogramInfo] = {
    val assumeBinaryIsString = sparkSession.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sparkSession.sessionState.conf.isParquetINT96AsTimestamp
    val writeLegacyParquetFormat = sparkSession.sessionState.conf.writeLegacyParquetFormat
    val serializedConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf())

    // !! HACK ALERT !!
    //
    // Parquet requires `FileStatus`es to read footers.  Here we try to send cached `FileStatus`es
    // to executor side to avoid fetching them again.  However, `FileStatus` is not `Serializable`
    // but only `Writable`.  What makes it worse, for some reason, `FileStatus` doesn't play well
    // with `SerializableWritable[T]` and always causes a weird `IllegalStateException`.  These
    // facts virtually prevents us to serialize `FileStatus`es.
    //
    // Since Parquet only relies on path and length information of those `FileStatus`es to read
    // footers, here we just extract them (which can be easily serialized), send them to executor
    // side, and resemble fake `FileStatus`es there.
    val partialFileStatusInfo = filesToTouch.map(f => (f.getPath.toString, f.getLen))

    // Set the number of partitions to prevent following schema reads from generating many tasks
    // in case of a small number of parquet files.
    val numParallelism = Math.min(Math.max(partialFileStatusInfo.size, 1),
      sparkSession.sparkContext.defaultParallelism)

    // Issues a Spark job to read Parquet schema in parallel.
    sparkSession
      .sparkContext
      .parallelize(partialFileStatusInfo, numParallelism)
      .mapPartitions { iterator =>
        // Resembles fake `FileStatus`es with serialized path and length information.
        val fakeFileStatuses = iterator.map { case (path, length) =>
          new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(path))
        }.toSeq

        // Skips row group information since we only need the schema
        val skipRowGroups = false

        import scala.collection.JavaConverters._

        // Reads footers in multi-threaded manner within each task
        val footers: Seq[Footer] =
        ParquetFileReader.readAllFootersInParallel(
          serializedConf.value, fakeFileStatuses.asJava, skipRowGroups).asScala

        if (footers.isEmpty) {
          Iterator.empty
        } else {
          footers.flatMap({ footer =>
            val blocks: Seq[BlockMetaData] = footer.getParquetMetadata.getBlocks.asScala
            val fileName = footer.getFile.getName();
            blocks.map({block =>
              val start = block.getStartingPos()
              val length = block.getTotalByteSize()

              val map = scala.collection.mutable.HashMap.empty[String, HistogramStatistics[Long]]
              for(x <- block.getColumns().asScala) {
                val colName = x.getPath().toString()
                  .substring(1, x.getPath().toString().length() - 1)
                map += (colName -> x.getStatistics().asInstanceOf[HistogramStatistics[Long]])
              }

              // Get Host here

              RowGroupHistogramInfo(fileName, "", start, length, map, Array(), partitionValue)
            })
          }).toIterator
        }
      }.collect()
  }

  /**
    *
    * @param inRanges
    * @param colName
    * @param histogram
    * @return the quality of a single colName
    */
  def getQuality(inRanges: Seq[InRange],
                 colName: String,
                 histogram: HistogramStatistics[Long]): Long = {
    var histogram_ret:Long=0L
    for(inrange <- inRanges if inrange.columnName==colName){
        histogram_ret=histogram.Quality(inrange.getLower, inrange.getUpper)
    }
    histogram_ret
  }

  /**
    *
    * @param inRanges
    * @param histograms
    * @param filters
    * @return pattern matching to get the quality of the all columns in a row group
    *         recursively, according to the filters
    *
    *         [CAUTIOUS] we only consider the query with Eq,Lt,Gt conditions here
    */
  def sumQuality(inRanges: Seq[InRange],
                 histograms: collection.mutable.HashMap[String, HistogramStatistics[Long]],
                 filters: Filter): Long = filters match{
    case org.apache.spark.sql.sources.And(left, right) => Math.min(sumQuality(inRanges, histograms, left), sumQuality(inRanges, histograms, right))
    case org.apache.spark.sql.sources.Or(left, right) => Math.max(sumQuality(inRanges, histograms, left), sumQuality(inRanges, histograms, right))
    case org.apache.spark.sql.sources.GreaterThan(attribute,value) => getQuality(inRanges, attribute, histograms(attribute))
    case org.apache.spark.sql.sources.LessThan(attribute, value) => getQuality(inRanges, attribute, histograms(attribute))
    case org.apache.spark.sql.sources.EqualTo(attribute, value) => getQuality(inRanges, attribute, histograms(attribute))
    case _ => 0L
  }


  /**
    * Sorting the a list of [RowGroupHistogramInfo] given the critera
    *
    * @param rowGroups
    * @param inRanges
    * @return
    */
  def sortingRowGroups(rowGroups: Seq[RowGroupHistogramInfo],
                       inRanges: Seq[InRange], filters: Filter): Seq[RowGroupHistogramInfo] = {
    val record_rowGroup_list = rowGroups.map({rowGroup =>
      val histograms = rowGroup.histograms

      /*
      loop through inRanges
      foreach column in inRange, find the # of records through statistic
      sort the ret and get lowest # of records to represent the RowGroup

      sort the RowGroups by the # of records
       */

      val Quality = sumQuality(inRanges, histograms, filters)
      /*
      val records_column = inRanges.map({column =>
        val histogram = histograms(column.columnName)
        val histogram_ret = histogram.Quality(column.getLower, column.getUpper)
        (histogram_ret, column)
      })

      val minRecord = records_column.minBy(_._1)._1

      (minRecord, rowGroup)
      */
      (Quality ,rowGroup)

    })

    val sorted = record_rowGroup_list.sortWith(_._1>_._1)
    sorted.foreach(println)
    sorted.map(_._2)
  }

  /**
    * Converting rowGroups to FilePartition
    * @param rowGroups
    * @return
    */
  def convertRowGroupsToFilePartition(rowGroups: Seq[RowGroupHistogramInfo]): Seq[FilePartition] = {

    rowGroups.zipWithIndex.map({rowGroupAndIndex =>
      val rowGroup = rowGroupAndIndex._1
      val index = rowGroupAndIndex._2

      FilePartition(index,
                    PartitionedFile(rowGroup.partitionValue, rowGroup.filePath,
                      rowGroup.start, rowGroup.length, rowGroup.hosts)::Nil)
    })
  }

}
