package org.apache.spark.sql.execution.datasources.parquet


import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.column.statistics.histogram.HistogramStatistics
import org.apache.parquet.filter2.statisticslevel.InRange
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.{Footer, ParquetFileReader}
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration



case class RowGroupHistogramInfo[T](filePath: String, start: Long, end: Long, histograms: Map[String, HistogramStatistics[T]])

object ParquetHistogramUtil{

  def getFilterObjects(filters:Filter): Iterable[InRange] = {
    val planText: String = filters.toString
    val gt = """GreaterThan\((\w),(\d*)\)""".r
    val lt = """LessThan\((\w),(\d*)\)""".r
    val equal = """EqualTo\((\w),(\d*)\)""".r

    //A map where key = column name , value = [low bound, up bound]
    val gt_lt_map = collection.mutable.HashMap.empty[String, (Long, Long)]

    //lower bound
    for (x <- gt.findAllMatchIn(planText)) {
      gt_lt_map += (x.group(0) -> (x.group(1).toLong, Long.MaxValue))
    }

    //upper bound
    for (x <- lt.findAllMatchIn(planText)) {
      val columnName = x.group(0)
      val tmp = gt_lt_map(columnName)
      gt_lt_map += (columnName -> (tmp._1, x.group(1).toLong))
    }

    val gt_lt_inRange = gt_lt_map.map({ k_v =>
      val colName = k_v._1
      val upper_lower = k_v._2

      val inRange = new InRange(colName)
      inRange.setLower(upper_lower._1)
      inRange.setUpper(upper_lower._2)

      inRange
    })

    val equal_inRange = equal.findAllMatchIn(planText).map({ x=>
      val inRange = new InRange(x.group(0))
      inRange.setLower(x.group(1).toLong)
      inRange.setUpper(x.group(1).toLong)
      inRange
    })

    gt_lt_inRange ++ equal_inRange
  }

  def getRowGroupHistogramInfoSeq(
                              filesToTouch: Seq[FileStatus],
                              sparkSession: SparkSession,
                              filter: Filter): Seq[RowGroupHistogramInfo] = {
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

    val targetFilterColumnName = filter\

    // Issues a Spark job to read Parquet schema in parallel.
    val partiallyMergedSchemas =
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

        import collection.JavaConversions._

        // Reads footers in multi-threaded manner within each task
        val footers: Seq[Footer] =
        ParquetFileReader.readAllFootersInParallel(
          serializedConf.value, fakeFileStatuses.asJava, skipRowGroups).asScala

        // Converter used to convert Parquet `MessageType` to Spark SQL `StructType`
        val converter =
        new ParquetSchemaConverter(
          assumeBinaryIsString = assumeBinaryIsString,
          assumeInt96IsTimestamp = assumeInt96IsTimestamp,
          writeLegacyParquetFormat = writeLegacyParquetFormat)

        if (footers.isEmpty) {
          Iterator.empty
        } else {
          footers.flatMap({footer =>
            val blocks: Seq[BlockMetaData] = footer.getParquetMetadata.getBlocks.asScala
            blocks.map({block =>

            })
            Seq()
          })
        }
        Iterator.single(mergedSchema)
      }
    }.collect()

    if (partiallyMergedSchemas.isEmpty) {
      None
    } else {
      var finalSchema = partiallyMergedSchemas.head
      partiallyMergedSchemas.tail.foreach { schema =>
        try {
          finalSchema = finalSchema.merge(schema)
        } catch { case cause: SparkException =>
          throw new SparkException(
            s"Failed merging schema:\n${schema.treeString}", cause)
        }
      }
      Some(finalSchema)
    }
  }
}
getFooters(){
  /**
    * ask executor get footer and [RowGroupHistogramInfo]
    *
    * then sort the RowGroupHistogramInfo according to `where` clauses
    *
    * Return a list of [FilePartition], each FilePartiton == RowGroup
    */
}