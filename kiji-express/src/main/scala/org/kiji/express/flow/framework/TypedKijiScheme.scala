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

import cascading.scheme.SourceCall
import cascading.scheme.SinkCall
import cascading.flow.FlowProcess
import cascading.tap.Tap
import cascading.tuple.Tuple

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.JobConf

import org.kiji.schema.KijiURI
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.express.flow.TimeRangeSpec
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.RowRangeSpec
import org.kiji.express.flow.RowFilterSpec
import org.kiji.express.flow.ExpressColumnOutput
import org.kiji.express.flow.ExpressResult
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.express.flow.framework.serialization.KijiKryoExternalizer
import org.kiji.express.flow.util.ResourceUtil.withKijiTable


class TypedKijiScheme(
  private[express] val tableAddress: String,
  private[express] val timeRange: TimeRangeSpec,
  icolumns: List[ColumnInputSpec] = List(),
  private[express] val rowRangeSpec: RowRangeSpec,
  private[express] val rowFilterSpec: RowFilterSpec
  ) extends BaseKijiScheme {

  import TypedKijiScheme._

  private[this] val _inputColumns = KijiKryoExternalizer(icolumns)

  def inputColumns: List[ColumnInputSpec] = _inputColumns.get
  private def uri: KijiURI = KijiURI.newBuilder(tableAddress).build()

  /**
   *
   * @param flow
   * @param tap
   * @param conf
   */
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

  /**
   *
   * @param flow
   * @param sourceCall
   * @return
   */
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

      sourceCall.getIncomingEntry.setTuple(result)
      flow.increment(BaseKijiScheme.CounterGroupName, BaseKijiScheme.CounterSuccess, 1)
      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the RecordReader.
    }
  }

  /**
   *
   * @param flow
   * @param sinkCall
   */
  override def sinkPrepare(
    flow: FlowProcess[JobConf],
    sinkCall: SinkCall[DirectKijiSinkContext, OutputCollector[_, _]]
    ):Unit =  {
    withKijiTable(uri, flow.getConfigCopy) { table =>
      // Set the sink context to an opened KijiTableWriter.
      sinkCall.setContext(
        DirectKijiSinkContext(
          EntityIdFactory.getFactory(table.getLayout),
          table.getWriterFactory.openBufferedWriter()))
    }
  }

  /**
   *
   * @param flow
   * @param sinkCall
   */
  override def sink(
    flow: FlowProcess[JobConf],
    sinkCall: SinkCall[DirectKijiSinkContext, OutputCollector[_, _]]
    ): Unit = {

    val DirectKijiSinkContext(eidFactory, writer) = sinkCall.getContext
    //The first object in tuple entry contains the data in the pipe.
    val typedPipeVal: Product = sinkCall.getOutgoingEntry.getObject(0).asInstanceOf[Product]
    typedPipeVal match {
      //Value being written to a single column.
      case singleVal: ExpressColumnOutput[_] =>
        writer.put(
          singleVal.entityId.toJavaEntityId(eidFactory),
          singleVal.family,
          singleVal.qualifier,
          singleVal.encode(singleVal.datum)
        )
      //Value being written to multiple columns.
      case nValTuple: Product =>
        nValTuple.productIterator.toList.foreach { anyVal =>
          val singleVal = anyVal.asInstanceOf[ExpressColumnOutput[_]]
          writer.put(
            singleVal.entityId.toJavaEntityId(eidFactory),
            singleVal.family,
            singleVal.qualifier,
            singleVal.encode(singleVal.datum)
          )
        }
    }
  }
}

/**
 * Companion object for [[TypedKijiScheme]] containing utility methods.
 */
object TypedKijiScheme {
  private[express] def rowToTuple(row: KijiRowData): Tuple = {
    val result: Tuple = new Tuple()
    result.add(ExpressResult(row))
    result
  }
}
