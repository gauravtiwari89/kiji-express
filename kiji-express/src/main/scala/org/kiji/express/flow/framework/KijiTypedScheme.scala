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

import cascading.scheme.{SourceCall, SinkCall}
import cascading.flow.FlowProcess
import cascading.tap.Tap
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}
import org.kiji.schema.{EntityId => JEntityId, _}
import org.kiji.express.flow.framework.serialization.KijiKryoExternalizer
import org.kiji.express.flow.util.ResourceUtil._
import org.kiji.mapreduce.framework.KijiConfKeys
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import cascading.tuple.Tuple
import org.kiji.express.flow._


class KijiTypedScheme(
  private[express] val tableAddress: String,
  private[express] val timeRange: TimeRangeSpec,
  icolumns: List[ColumnInputSpec] = List(),
  ocolumns: List[ColumnOutputSpec] = List(),
  private[express] val rowRangeSpec: RowRangeSpec,
  private[express] val rowFilterSpec: RowFilterSpec
  )
  extends BaseKijiScheme {
  import KijiTypedScheme._

  /** Serialization workaround.  Do not access directly. */
  private[this] val _inputColumns = KijiKryoExternalizer(icolumns)
  private[this] val _outputColumns = KijiKryoExternalizer(ocolumns)

  def inputColumns: List[ColumnInputSpec] = _inputColumns.get

  def outputColumns: List[ColumnOutputSpec] = _outputColumns.get

  private def uri: KijiURI = KijiURI.newBuilder(tableAddress).build()


  override def sourceConfInit(
    flow: FlowProcess[JobConf],
    tap: Tap[
      JobConf,
      RecordReader[Container[JEntityId], Container[KijiRowData]],
      OutputCollector[_, _]
      ],
    conf: JobConf): Unit = {

    // Build a data request.
    val request: KijiDataRequest = withKijiTable(uri, conf) { table =>
      BaseKijiScheme.buildRequest(table.getLayout, timeRange, inputColumns)
    }
    // Write all the required values to the job's configuration object.
    configureRequest(uri, conf, rowRangeSpec, rowFilterSpec)
    // Set data request.
    conf.set(
      KijiConfKeys.KIJI_INPUT_DATA_REQUEST,
      Base64.encodeBase64String(SerializationUtils.serialize(request)))
  }


  override def source(
    flow: FlowProcess[JobConf],
    sourceCall: SourceCall[
      KijiSourceContext,
      RecordReader[Container[JEntityId], Container[KijiRowData]]
      ]
    ): Boolean = {

    // Get the current key/value pair.
    val rowContainer = sourceCall.getContext.rowContainer

    // Get the next row.
    if (sourceCall.getInput.next(null, rowContainer)) {
      val row: KijiRowData = rowContainer.getContents

      // Build a tuple from this row.
      val result: Tuple = rowToTuple(row)

      // If no fields were missing, set the result tuple and return from this method.
      sourceCall.getIncomingEntry.setTuple(result)
      flow.increment(BaseKijiScheme.CounterGroupName, BaseKijiScheme.CounterSuccess, 1)

      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the RecordReader.
    }
  }


  //To be implemented.
  override def sink(
    flow: FlowProcess[JobConf],
    sinkCall: SinkCall[DirectKijiSinkContext, OutputCollector[_, _]]): Unit = ???


  override def sinkConfInit(
    flow: FlowProcess[JobConf],
    tap: Tap[
      JobConf,
      RecordReader[Container[JEntityId], Container[KijiRowData]],
      OutputCollector[_, _]
      ],
    conf: JobConf
    ): Unit = ???


  //================================HELPER METHODS==================

}

object KijiTypedScheme {

  private[express] def rowToTuple(
    row: KijiRowData
    ): Tuple = {
    val result: Tuple = new Tuple()
    result.add(row)
    result
  }

}




