/**
 * (c) Copyright 2013 WibiData, Inc.
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

import cascading.scheme.{SinkCall, SourceCall}
import java.io.{OutputStream, InputStream}
import java.util.Properties
import org.kiji.express.flow._
import org.kiji.schema.{KijiRowData, EntityIdFactory, KijiTable, KijiURI}
import cascading.flow.FlowProcess
import cascading.tap.Tap
import org.apache.hadoop.mapred.JobConf
import cascading.flow.hadoop.util.HadoopUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.kiji.express.flow.util.ResourceUtil._
import scala.Some
import org.kiji.schema.KijiTableReader.KijiScannerOptions
import scala.collection.JavaConverters.asScalaIteratorConverter
import cascading.tuple.Tuple

private [express] case class TypedLocalKijiScheme(
  private[express] val uri: KijiURI,
  private[express] val timeRange: TimeRangeSpec,
  private[express] val inputColumns: List[ColumnInputSpec] = List(),
  private[express] val outputColumns: List[ColumnOutputSpec] = List(),
  private[express] val rowRangeSpec: RowRangeSpec,
  private[express] val rowFilterSpec: RowFilterSpec)
  extends BaseLocalKijiScheme
  {

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
      val result: Tuple = KijiTypedScheme.rowToTuple(row)

      // Set the result tuple and return from this method.
      sourceCall.getIncomingEntry.setTuple(result)
      process.increment(BaseKijiScheme.CounterGroupName, BaseKijiScheme.CounterSuccess, 1)
      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the input.
    }
  }


  override def sourceConfInit(
    process: FlowProcess[Properties],
    tap: Tap[Properties, InputStream, OutputStream],
    conf: Properties) {
    // No-op. Setting options in a java Properties object is not going to help us read from
    // a Kiji table.
  }

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

  override def sink(
    flowProcess: FlowProcess[Properties],
    sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]): Unit = ???
}
