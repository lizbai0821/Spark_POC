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

package org.apache.spark.sql.execution

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetHistogramUtil, ParquetFileFormat => ParquetSource}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer
// import scala.collection.mutable.ArrayBuffer

trait DataSourceScanExec extends LeafExecNode with CodegenSupport {
  val relation: BaseRelation
  val metastoreTableIdentifier: Option[TableIdentifier]

  override val nodeName: String = {
    s"Scan $relation ${metastoreTableIdentifier.map(_.unquotedString).getOrElse("")}"
  }
}

/** Physical plan node for scanning data from a relation. */
case class RowDataSourceScanExec(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    override val outputPartitioning: Partitioning,
    override val metadata: Map[String, String],
    override val metastoreTableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec {

  override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  val outputUnsafeRows = relation match {
    case r: HadoopFsRelation if r.fileFormat.isInstanceOf[ParquetSource] =>
      !SparkSession.getActiveSession.get.sessionState.conf.getConf(
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED)
    case _: HadoopFsRelation => true
    case _ => false
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val unsafeRow = if (outputUnsafeRows) {
      rdd
    } else {
      rdd.mapPartitionsInternal { iter =>
        val proj = UnsafeProjection.create(schema)
        iter.map(proj)
      }
    }

    val numOutputRows = longMetric("numOutputRows")
    unsafeRow.map { r =>
      numOutputRows += 1
      r
    }
  }

  override def simpleString: String = {
    val metadataEntries = for ((key, value) <- metadata.toSeq.sorted) yield {
      key + ": " + StringUtils.abbreviate(value, 100)
    }

    s"$nodeName${Utils.truncatedString(output, "[", ",", "]")}" +
      s"${Utils.truncatedString(metadataEntries, " ", ", ", "")}"
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    // PhysicalRDD always just has one input
    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val exprRows = output.zipWithIndex.map{ case (a, i) =>
      new BoundReference(i, a.dataType, a.nullable)
    }
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsRowInput = exprRows.map(_.genCode(ctx))
    val inputRow = if (outputUnsafeRows) row else null
    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  $numOutputRows.add(1);
       |  ${consume(ctx, columnsRowInput, inputRow).trim}
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }

  // Ignore rdd when checking results
  override def sameResult(plan: SparkPlan): Boolean = plan match {
    case other: RowDataSourceScanExec => relation == other.relation && metadata == other.metadata
    case _ => false
  }
}

/**
 * Physical plan node for scanning data from HadoopFsRelations.
 *
 * @param relation The file-based relation to scan.
 * @param output Output attributes of the scan.
 * @param outputSchema Output schema of the scan.
 * @param partitionFilters Predicates to use for partition pruning.
 * @param dataFilters Data source filters to use for filtering data within partitions.
 * @param metastoreTableIdentifier
 */
case class FileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    outputSchema: StructType,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Filter],
    override val metastoreTableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec {

  val supportsBatch = relation.fileFormat.supportBatch(
    relation.sparkSession, StructType.fromAttributes(output))

  val needsUnsafeRowConversion = if (relation.fileFormat.isInstanceOf[ParquetSource]) {
    SparkSession.getActiveSession.get.sessionState.conf.parquetVectorizedReaderEnabled
  } else {
    false
  }

  override val outputPartitioning: Partitioning = {
    val bucketSpec = if (relation.sparkSession.sessionState.conf.bucketingEnabled) {
      relation.bucketSpec
    } else {
      None
    }
    bucketSpec.map { spec =>
      val numBuckets = spec.numBuckets
      val bucketColumns = spec.bucketColumnNames.flatMap { n =>
        output.find(_.name == n)
      }
      if (bucketColumns.size == spec.bucketColumnNames.size) {
        HashPartitioning(bucketColumns, numBuckets)
      } else {
        UnknownPartitioning(0)
      }
    }.getOrElse {
      UnknownPartitioning(0)
    }
  }

  // These metadata values make scan plans uniquely identifiable for equality checking.
  override val metadata: Map[String, String] = Map(
    "Format" -> relation.fileFormat.toString,
    "ReadSchema" -> outputSchema.catalogString,
    "Batched" -> supportsBatch.toString,
    "PartitionFilters" -> partitionFilters.mkString("[", ", ", "]"),
    "PushedFilters" -> dataFilters.mkString("[", ", ", "]"),
    "InputPaths" -> relation.location.paths.mkString(", "))

  private lazy val inputRDD: RDD[InternalRow] = {
    val selectedPartitions = relation.location.listFiles(partitionFilters)

    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = outputSchema,
        filters = dataFilters,
        options = relation.options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    val useHistogram:Boolean = ParquetHistogramUtil.useHistogram(dataFilters)

    if (relation.fileFormat.isInstanceOf[ParquetSource] && dataFilters.size > 0){
      logInfo("Build custom parquet inputRDD, enable histogram sorting")
      createParquetHistogramInputRDD(readFile, selectedPartitions, relation)
    }
    else{
      relation.bucketSpec match {
        case Some(bucketing) if relation.sparkSession.sessionState.conf.bucketingEnabled =>
          createBucketedReadRDD(bucketing, readFile, selectedPartitions, relation)
        case _ =>
          createNonBucketedReadRDD(readFile, selectedPartitions, relation)
      }
    }

  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))

  protected override def doExecute(): RDD[InternalRow] = {
    if (supportsBatch) {
      // in the case of fallback, this batched scan should never fail because of:
      // 1) only primitive types are supported
      // 2) the number of columns should be smaller than spark.sql.codegen.maxFields
      WholeStageCodegenExec(this).execute()
    } else {
      val unsafeRows = {
        val scan = inputRDD
        if (needsUnsafeRowConversion) {
          scan.mapPartitionsInternal { iter =>
            val proj = UnsafeProjection.create(schema)
            iter.map(proj)
          }
        } else {
          scan
        }
      }
      val numOutputRows = longMetric("numOutputRows")
      unsafeRows.map { r =>
        numOutputRows += 1
        r
      }
    }
  }

  override def simpleString: String = {
    val metadataEntries = for ((key, value) <- metadata.toSeq.sorted) yield {
      key + ": " + StringUtils.abbreviate(value, 100)
    }
    val metadataStr = Utils.truncatedString(metadataEntries, " ", ", ", "")
    s"File$nodeName${Utils.truncatedString(output, "[", ",", "]")}$metadataStr"
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    if (supportsBatch) {
      return doProduceVectorized(ctx)
    }
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    // PhysicalRDD always just has one input
    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val exprRows = output.zipWithIndex.map{ case (a, i) =>
      new BoundReference(i, a.dataType, a.nullable)
    }
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsRowInput = exprRows.map(_.genCode(ctx))
    val inputRow = if (needsUnsafeRowConversion) null else row
    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  $numOutputRows.add(1);
       |  ${consume(ctx, columnsRowInput, inputRow).trim}
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }

  // Support codegen so that we can avoid the UnsafeRow conversion in all cases. Codegen
  // never requires UnsafeRow as input.
  private def doProduceVectorized(ctx: CodegenContext): String = {
    val input = ctx.freshName("input")
    // PhysicalRDD always just has one input
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")

    // metrics
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val scanTimeMetric = metricTerm(ctx, "scanTime")
    val scanTimeTotalNs = ctx.freshName("scanTime")
    ctx.addMutableState("long", scanTimeTotalNs, s"$scanTimeTotalNs = 0;")

    val columnarBatchClz = "org.apache.spark.sql.execution.vectorized.ColumnarBatch"
    val batch = ctx.freshName("batch")
    ctx.addMutableState(columnarBatchClz, batch, s"$batch = null;")

    val columnVectorClz = "org.apache.spark.sql.execution.vectorized.ColumnVector"
    val idx = ctx.freshName("batchIdx")
    ctx.addMutableState("int", idx, s"$idx = 0;")
    val colVars = output.indices.map(i => ctx.freshName("colInstance" + i))
    val columnAssigns = colVars.zipWithIndex.map { case (name, i) =>
      ctx.addMutableState(columnVectorClz, name, s"$name = null;")
      s"$name = $batch.column($i);"
    }

    val nextBatch = ctx.freshName("nextBatch")
    ctx.addNewFunction(nextBatch,
      s"""
         |private void $nextBatch() throws java.io.IOException {
         |  long getBatchStart = System.nanoTime();
         |  if ($input.hasNext()) {
         |    $batch = ($columnarBatchClz)$input.next();
         |    $numOutputRows.add($batch.numRows());
         |    $idx = 0;
         |    ${columnAssigns.mkString("", "\n", "\n")}
         |  }
         |  $scanTimeTotalNs += System.nanoTime() - getBatchStart;
         |}""".stripMargin)

    ctx.currentVars = null
    val rowidx = ctx.freshName("rowIdx")
    val columnsBatchInput = (output zip colVars).map { case (attr, colVar) =>
      genCodeColumnVector(ctx, colVar, rowidx, attr.dataType, attr.nullable)
    }
    s"""
       |if ($batch == null) {
       |  $nextBatch();
       |}
       |while ($batch != null) {
       |  int numRows = $batch.numRows();
       |  while ($idx < numRows) {
       |    int $rowidx = $idx++;
       |    ${consume(ctx, columnsBatchInput).trim}
       |    if (shouldStop()) return;
       |  }
       |  $batch = null;
       |  $nextBatch();
       |}
       |$scanTimeMetric.add($scanTimeTotalNs / (1000 * 1000));
       |$scanTimeTotalNs = 0;
     """.stripMargin
  }

  private def genCodeColumnVector(ctx: CodegenContext, columnVar: String, ordinal: String,
    dataType: DataType, nullable: Boolean): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val value = ctx.getValue(columnVar, dataType, ordinal)
    val isNullVar = if (nullable) { ctx.freshName("isNull") } else { "false" }
    val valueVar = ctx.freshName("value")
    val str = s"columnVector[$columnVar, $ordinal, ${dataType.simpleString}]"
    val code = s"${ctx.registerComment(str)}\n" + (if (nullable) {
      s"""
        boolean ${isNullVar} = ${columnVar}.isNullAt($ordinal);
        $javaType ${valueVar} = ${isNullVar} ? ${ctx.defaultValue(dataType)} : ($value);
      """
    } else {
      s"$javaType ${valueVar} = $value;"
    }).trim
    ExprCode(code, isNullVar, valueVar)
  }

  /**
   * Create an RDD for bucketed reads.
   * The non-bucketed variant of this function is [[createNonBucketedReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec the bucketing spec.
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Seq[Partition],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val bucketed =
      selectedPartitions.flatMap { p =>
        p.files.map { f =>
          val hosts = getBlockHosts(getBlockLocations(f), 0, f.getLen)
          PartitionedFile(p.values, f.getPath.toUri.toString, 0, f.getLen, hosts)
        }
      }.groupBy { f =>
        BucketingUtils
          .getBucketId(new Path(f.filePath).getName)
          .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
      }

    val filePartitions = Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
      FilePartition(bucketId, bucketed.getOrElse(bucketId, Nil))
    }

    new FileScanRDD(fsRelation.sparkSession, readFile, filePartitions)
  }

  /**
   * Create an RDD for non-bucketed reads.
   * The bucketed variant of this function is [[createBucketedReadRDD]].
   *
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createNonBucketedReadRDD(
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Seq[Partition],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    val defaultMaxSplitBytes =
      fsRelation.sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = fsRelation.sparkSession.sparkContext.defaultParallelism
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        val blockLocations = getBlockLocations(file)
        if (fsRelation.fileFormat.isSplitable(
            fsRelation.sparkSession, fsRelation.options, file.getPath)) {
          (0L until file.getLen by maxSplitBytes).map { offset =>
            val remaining = file.getLen - offset
            val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
            val hosts = getBlockHosts(blockLocations, offset, size)
            PartitionedFile(
              partition.values, file.getPath.toUri.toString, offset, size, hosts)
          }
        } else {
          val hosts = getBlockHosts(blockLocations, 0, file.getLen)
          Seq(PartitionedFile(
            partition.values, file.getPath.toUri.toString, 0, file.getLen, hosts))
        }
      }
    }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        val newPartition =
          FilePartition(
            partitions.size,
            currentFiles.toArray.toSeq) // Copy to a new Array.
        partitions.append(newPartition)
      }
      currentFiles.clear()
      currentSize = 0
    }

    // Assign files to partitions using "First Fit Decreasing" (FFD)
    // TODO: consider adding a slop factor here?
    splitFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles.append(file)
    }
    closePartition()

    new FileScanRDD(fsRelation.sparkSession, readFile, partitions)
  }

  /**
    * Create an RDD for non-bucketed reads.
    * The bucketed variant of this function is [[createBucketedReadRDD]].
    *
    * @param readFile a function to read each (part of a) file.
    * @param selectedPartitions Hive-style partition that are part of the read.
    * @param fsRelation [[HadoopFsRelation]] associated with the read.
    */
  private def createParquetHistogramInputRDD(
                                        readFile: (PartitionedFile) => Iterator[InternalRow],
                                        selectedPartitions: Seq[Partition],
                                        fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo("Spark-dist 4. Cache footer in driver")
    val defaultMaxSplitBytes =
      fsRelation.sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = fsRelation.sparkSession.sparkContext.defaultParallelism
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val inRanges = ParquetHistogramUtil.getFilterObjects(dataFilters(0))

    val colNames = inRanges.map(_.columnName).toSet

    val rowGroups =
      if(ParquetHistogramUtil.isCachedRowGroupMetadata) {
        logInfo("Parquet row group metadata cache is used")
        val targetPartitionValues = selectedPartitions.map(_.values.toString).toSet

        // get Row Groups from cached and filter the partiton key
        ParquetHistogramUtil.getCachedRowGroupMetadata()
          .filter(rowGroup => targetPartitionValues
            .contains(rowGroup.partitionValue.toString))
      }else{
        val rowGroupMetadata = selectedPartitions.flatMap { partition =>
          val _rowGroups = ParquetHistogramUtil.getRowGroupHistogramInfoSeq(partition.files,
            SparkSession.getActiveSession.get, partition.values)
          // Mapping for RowGroup path and FileStatus
          val mapping = partition.files.map(file => file.getPath.getName -> file).toMap
          _rowGroups.map({ rowGroup =>
            val fileStatus = mapping(rowGroup.fileName)
            rowGroup.hosts = getBlockHosts(getBlockLocations(
              fileStatus), rowGroup.start, rowGroup.length)
            rowGroup.filePath = fileStatus.getPath.toUri().toString()
            rowGroup
          })
        }
        ParquetHistogramUtil.setCachedRowGroupMetadata(rowGroupMetadata)
        rowGroupMetadata
      }

    val sortedRowGroups = ParquetHistogramUtil.sortingRowGroups(rowGroups, inRanges, dataFilters(0))

    new FileScanRDD(fsRelation.sparkSession, readFile,
      ParquetHistogramUtil.convertRowGroupsToFilePartition(sortedRowGroups))
  }


  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  private def getBlockHosts(
      blockLocations: Array[BlockLocation], offset: Long, length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if offset <= b.getOffset && offset + length < b.getLength =>
        b.getHosts -> (offset + length - b.getOffset).min(length)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }

  override def sameResult(plan: SparkPlan): Boolean = plan match {
    case other: FileSourceScanExec =>
      val thisPredicates = partitionFilters.map(cleanExpression)
      val otherPredicates = other.partitionFilters.map(cleanExpression)
      val result = relation == other.relation && metadata == other.metadata &&
        thisPredicates.length == otherPredicates.length &&
        thisPredicates.zip(otherPredicates).forall(p => p._1.semanticEquals(p._2))
      result
    case _ => false
  }
}

