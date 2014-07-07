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

import cascading.scheme.{SinkCall, SourceCall, Scheme}
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}
import org.kiji.schema.{EntityId => JEntityId, _}
import cascading.flow.FlowProcess
import org.kiji.schema.layout.{ColumnReaderSpec, KijiTableLayout}
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.avro.SchemaType
import org.kiji.annotations.{ApiStability, ApiAudience}
import cascading.tap.Tap
import org.kiji.express.flow.util.ResourceUtil._
import org.kiji.mapreduce.framework.KijiConfKeys
import org.apache.commons.codec.binary.Base64
import org.kiji.express.flow._
import scala.Some

trait BaseKijiScheme extends Scheme[
  JobConf,
  RecordReader[Container[JEntityId], Container[KijiRowData]],
  OutputCollector[_, _],
  KijiSourceContext,
  DirectKijiSinkContext
  ]{

  /**
   * Sets up any resources required for the MapReduce job. This method is called on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourcePrepare(
    flow: FlowProcess[JobConf],
    sourceCall: SourceCall[
      KijiSourceContext,
      RecordReader[Container[JEntityId], Container[KijiRowData]]
      ]
    ) {
    // Set the context used when reading data from the source.
    sourceCall.setContext(KijiSourceContext(sourceCall.getInput.createValue()))
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow currently being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourceCleanup(
    flow: FlowProcess[JobConf],
    sourceCall: SourceCall[
      KijiSourceContext,
      RecordReader[Container[JEntityId], Container[KijiRowData]]
      ]
    ) {
    sourceCall.setContext(null)
  }


  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param tap that is being used with this scheme.
   * @param conf to which we will add our KijiDataRequest.
   */
  override def sinkConfInit(
    flow: FlowProcess[JobConf],
    tap: Tap[
      JobConf,
      RecordReader[Container[JEntityId], Container[KijiRowData]],
      OutputCollector[_, _]
      ],
    conf: JobConf
    ) {
    // No-op since no configuration parameters need to be set to encode data for Kiji.
  }


  /**
   * Cleans up any resources used during the MapReduce job. This method is called on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkCleanup(
    flow: FlowProcess[JobConf],
    sinkCall: SinkCall[DirectKijiSinkContext, OutputCollector[_, _]]) {
    val writer = sinkCall.getContext.writer
    writer.flush()
    writer.close()
    sinkCall.setContext(null)
  }

  def configureRequest(uri:KijiURI,conf:JobConf, rowRangeSpec: RowRangeSpec, rowFilterSpec:RowFilterSpec){
    val eidFactory = withKijiTable(uri, conf) { table =>
      EntityIdFactory.getFactory(table.getLayout())
    }
    // Set start entity id.
    rowRangeSpec.startEntityId match {
      case Some(entityId) => {
        conf.set(
          KijiConfKeys.KIJI_START_ROW_KEY,
          Base64.encodeBase64String(
            entityId.toJavaEntityId(eidFactory).getHBaseRowKey()))
      }
      case None => {}
    }
    // Set limit entity id.
    rowRangeSpec.limitEntityId match {
      case Some(entityId) => {
        conf.set(
          KijiConfKeys.KIJI_LIMIT_ROW_KEY,
          Base64.encodeBase64String(
            entityId.toJavaEntityId(eidFactory).getHBaseRowKey()))
      }
      case None => {}
    }
    // Set row filter.
    rowFilterSpec.toKijiRowFilter match {
      case Some(kijiRowFilter) => {
        conf.set(KijiConfKeys.KIJI_ROW_FILTER, kijiRowFilter.toJson.toString)
      }
      case None => {}
    }
  }



}

object BaseKijiScheme {

  /** Hadoop mapred counter group for KijiExpress. */
  private[express] val CounterGroupName = "kiji-express"
  /** Counter name for the number of rows successfully read. */
  private[express] val CounterSuccess = "ROWS_SUCCESSFULLY_READ"

  /**
   * Builds a data request out of the timerange and list of column requests.
   *
   * @param timeRange of cells to retrieve.
   * @param columns to retrieve.
   * @return data request configured with timeRange and columns.
   */
  private[express] def buildRequest(
    layout: KijiTableLayout,
    timeRange: TimeRangeSpec,
    columns: Iterable[ColumnInputSpec]
    ): KijiDataRequest = {
    def addColumn(
      builder: KijiDataRequestBuilder,
      column: ColumnInputSpec
      ): KijiDataRequestBuilder.ColumnsDef = {
      val kijiFilter: KijiColumnFilter = column
        .filterSpec
        .toKijiColumnFilter
        .getOrElse(null)
      val columnReaderSpec: ColumnReaderSpec = {
        // Check and ensure that this column isn't a counter, protobuf, or raw bytes encoded column.
        // If it is, ignore the provided schema spec.
        val schemaType = column match {
          case QualifiedColumnInputSpec(family, qualifier, _, _, _, _) => {
            // If this fully qualified column is actually part of a map-type column family,
            // then get the schema type from the map-type column family instead. Otherwise get it
            // from the qualified column as usual.
            val columnFamily = layout
              .getFamilyMap
              .get(column.columnName.getFamily)
            if (columnFamily.isMapType) {
              columnFamily
                .getDesc
                .getMapSchema
                .getType
            } else {
              columnFamily
                .getColumnMap
                .get(column.columnName.getQualifier)
                .getDesc
                .getColumnSchema
                .getType
            }
          }
          case ColumnFamilyInputSpec(family, _, _, _, _) => {
            layout
              .getFamilyMap
              .get(column.columnName.getFamily)
              .getDesc
              .getMapSchema
              .getType
          }
        }
        schemaType match {
          case SchemaType.COUNTER => ColumnReaderSpec.counter()
          case SchemaType.PROTOBUF => ColumnReaderSpec.protobuf()
          case SchemaType.RAW_BYTES => ColumnReaderSpec.bytes()
          case _ => column.schemaSpec match {
            case SchemaSpec.DefaultReader => ColumnReaderSpec.avroDefaultReaderSchemaGeneric()
            case SchemaSpec.Writer => ColumnReaderSpec.avroWriterSchemaGeneric()
            case SchemaSpec.Generic(schema) => ColumnReaderSpec.avroReaderSchemaGeneric(schema)
            case SchemaSpec.Specific(record) => ColumnReaderSpec.avroReaderSchemaSpecific(record)
          }
        }
      }

      builder.newColumnsDef()
        .withMaxVersions(column.maxVersions)
        .withFilter(kijiFilter)
        .withPageSize(column.pagingSpec.cellsPerPage.getOrElse(0))
        .add(column.columnName, columnReaderSpec)
    }

    val requestBuilder: KijiDataRequestBuilder = KijiDataRequest.builder()
      .withTimeRange(timeRange.begin, timeRange.end)

    columns.foreach(column => addColumn(requestBuilder, column))
    requestBuilder.build()
  }

}


/**
 * Container for a Kiji row data and Kiji table layout object that is required by a map reduce
 * task while reading from a Kiji table.
 *
 * @param rowContainer is the representation of a Kiji row.
 */
@ApiAudience.Private
@ApiStability.Stable
private[express] final case class KijiSourceContext(rowContainer: Container[KijiRowData])

/**
 * Container for the table writer and Kiji table layout required during a sink
 * operation to write the output of a map reduce task back to a Kiji table.
 * This is configured during the sink prepare operation.
 */
@ApiAudience.Private
@ApiStability.Stable
private[express] final case class DirectKijiSinkContext(
  eidFactory: EntityIdFactory,
  writer: KijiBufferedWriter)
