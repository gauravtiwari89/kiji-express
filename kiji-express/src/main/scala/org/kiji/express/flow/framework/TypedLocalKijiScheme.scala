/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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
package org.kiji.express.flow.framework

import java.io.OutputStream
import java.io.InputStream
import java.util.Properties

import scala.collection.JavaConverters.asScalaIteratorConverter

import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.flow.FlowProcess
import cascading.tuple.Tuple
import cascading.flow.hadoop.util.HadoopUtil
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.schema.EntityIdFactory
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.express.flow._
import org.kiji.express.flow.util.ResourceUtil.withKijiTable
import org.kiji.schema.KijiTableReader.KijiScannerOptions
import org.kiji.express.flow.ExpressColumnOutput
import scala.Some


/**
 * A local version of [[org.kiji.express.flow.framework.TypedKijiScheme]] that is meant to be used
 * with Cascading's local job runner.
 *
 * [[TypedLocalKijiScheme]] extends trait [[org.kiji.express.flow.framework.BaseLocalKijiScheme]]
 * which holds method implementations of [[cascading.scheme.Scheme]] that are common to both
 * [[LocalKijiScheme]] and [[TypedLocalKijiScheme]] for running jobs locally.
 *
 * This scheme is meant to be used with [[org.kiji.express.flow.framework.LocalKijiTap]] and
 * Cascading's local job runner. Jobs run with Cascading's local job runner execute on
 * your local machine instead of a cluster. This can be helpful for testing or quick jobs.
 *
 * This scheme is responsible for converting rows from a Kiji table that are input to a Cascading
 * flow into Cascading tuples
 * (see* `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`)
 * and writing output data from a Cascading flow to a Kiji table
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * @see[[org.kiji.express.flow.framework.TypedKijiScheme]]
 * @see[[org.kiji.express.flow.framework.BaseLocalKijiScheme]]
 *
 * @param uri of table to be written to.
 * @param timeRange to include from the Kiji table.Use None if all values should be written
 *     at the current time.
 * @param inputColumns is a one-to-one mapping from field names to Kiji columns. The columns in the
 *     map will be read into their associated tuple fields.
 */
private[express] case class TypedLocalKijiScheme(
  private[express] val uri: KijiURI,
  private[express] val timeRange: TimeRangeSpec,
  private[express] val inputColumns: List[ColumnInputSpec] = List(),
  private[express] val rowRangeSpec: RowRangeSpec,
  private[express] val rowFilterSpec: RowFilterSpec)
  extends BaseLocalKijiScheme {

  /**
   * Reads and converts a row from a Kiji table to a Cascading Tuple. This method
   * is called once for each row in the table.
   *
   * @param process is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
   * @return <code>true</code> if another row was read and it was converted to a tuple,
   *         <code>false</code> if there were no more rows to read.
   */
  override def source(
    process: FlowProcess[Properties],
    sourceCall: SourceCall[InputContext, InputStream]): Boolean = {
    val context: InputContext = sourceCall.getContext
    if (context.iterator.hasNext) {
      // Get the current row.
      val row: KijiRowData = context.iterator.next()
      val result: Tuple = TypedKijiScheme.rowToTuple(row)

      // Set the result tuple and return from this method.
      sourceCall.getIncomingEntry.setTuple(result)
      process.increment(BaseKijiScheme.CounterGroupName, BaseKijiScheme.CounterSuccess, 1)
      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the input.
    }
  }

  /**
   * Sets up any resources required to read from a Kiji table.
   *
   * @param process currently being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourcePrepare(
    process: FlowProcess[Properties],
    sourceCall: SourceCall[InputContext, InputStream]) {

    val conf: JobConf =
      HadoopUtil.createJobConf(process.getConfigCopy, new JobConf(HBaseConfiguration.create()))

    // Build the input context.
    withKijiTable(uri, conf) { table: KijiTable =>
      val request = BaseKijiScheme.buildRequest(table.getLayout, timeRange, inputColumns)
      val reader = BaseLocalKijiScheme.openReaderWithOverrides(table, request)

      // Set up scanning options.
      val eidFactory = EntityIdFactory.getFactory(table.getLayout())
      val scannerOptions = new KijiScannerOptions()
      scannerOptions.setKijiRowFilter(
        rowFilterSpec.toKijiRowFilter.getOrElse(null))
      scannerOptions.setStartRow(
        rowRangeSpec.startEntityId match {
          case Some(entityId) => entityId.toJavaEntityId(eidFactory)
          case None => null
        }
      )
      scannerOptions.setStopRow(
        rowRangeSpec.limitEntityId match {
          case Some(entityId) => entityId.toJavaEntityId(eidFactory)
          case None => null
        }
      )
      val scanner = reader.getScanner(request, scannerOptions)
      val tableUri = table.getURI
      val context = InputContext(reader, scanner, scanner.iterator.asScala, tableUri, conf)

      sourceCall.setContext(context)
    }
  }

  /**
   * Sets up any resources required to write to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkPrepare(
    process: FlowProcess[Properties],
    sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]) {
    val conf: JobConf =
      HadoopUtil.createJobConf(process.getConfigCopy, new JobConf(HBaseConfiguration.create()))
    withKijiTable(uri, conf) { table =>
      // Set the sink context to an opened KijiTableWriter.
      sinkCall.setContext(
        DirectKijiSinkContext(
          EntityIdFactory.getFactory(table.getLayout),
          table.getWriterFactory.openBufferedWriter())
      )
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Kiji table. The input type to the sink is expected
   * to be either a [[org.kiji.express.flow.ExpressColumnOutput]] object, or a Tuple of multiple
   * ExpressColumnOutput objects where each element in the tuple corresponds to a column of
   * the Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sink(
    process: FlowProcess[Properties],
    sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]) {
    val DirectKijiSinkContext(eidFactory, writer) = sinkCall.getContext
    //The first object in tuple entry contains the data in the pipe.
    val typedPipeVal: Product = sinkCall.getOutgoingEntry.getObject(0).asInstanceOf[Product]
    typedPipeVal match {
      //Value being written to a single column.
      case singleVal: ExpressColumnOutput[_] =>
        singleVal.timeStamp match {
          case Some(timestamp) =>
            //Timestamp specified.
            writer.put(
              singleVal.entityId.toJavaEntityId (eidFactory),
              singleVal.family,
              singleVal.qualifier,
              timestamp,
              singleVal.encode (singleVal.datum)
            )
          case None =>
            //Timestamp not specified.
            writer.put(
              singleVal.entityId.toJavaEntityId (eidFactory),
              singleVal.family,
              singleVal.qualifier,
              singleVal.encode (singleVal.datum)
            )
        }
      //Value being written to multiple columns.
      case nValTuple: Product =>
        nValTuple.productIterator.toList.foreach { anyVal =>
          val singleVal = anyVal.asInstanceOf[ExpressColumnOutput[_]]
          singleVal.timeStamp match {
            case Some(timestamp) =>
              writer.put(
                singleVal.entityId.toJavaEntityId (eidFactory),
                singleVal.family,
                singleVal.qualifier,
                timestamp,
                singleVal.encode (singleVal.datum)
              )
            case None =>
              writer.put(
                singleVal.entityId.toJavaEntityId (eidFactory),
                singleVal.family,
                singleVal.qualifier,
                singleVal.encode (singleVal.datum)
              )
          }
        }
    }
  }
}
