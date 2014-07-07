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
package org.kiji.express.flow

import com.twitter.scalding._
import cascading.tap.Tap
import org.kiji.express.flow.framework._
import org.kiji.schema._
import com.google.common.base.Objects

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable
import cascading.tuple.{TupleEntry, Tuple}
import cascading.flow.FlowProcess
import cascading.scheme.SinkCall
import java.util.Properties
import org.kiji.express.flow.util.ResourceUtil._
import java.io.OutputStream
import org.apache.hadoop.mapred.JobConf
import cascading.flow.hadoop.util.HadoopUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.kiji.schema.KijiTableReader.KijiScannerOptions
import scala.collection.mutable.Buffer
import org.apache.hadoop.conf.Configuration
import com.twitter.scalding.Test
import scala.Some
import org.kiji.express.flow.framework.DirectKijiSinkContext
import com.twitter.scalding.Hdfs
import org.kiji.express.flow.framework.TypedLocalKijiScheme


/**
 * TypedKijiSource is a TypeSafe representation of [[KijiSource]]. This class extends [[.Mappable]]
 * to allow compile time type checking in accordance with scalding's type safe API.
 *
 * @param tableAddress is a Kiji URI addressing the Kiji table to read or write to.
 * @param timeRange that cells read must belong to. Ignored when the source is used to write.
 * @param inputColumns is a one-to-one mapping from field names to Kiji columns. The columns in the
 *                     map will be read into their associated tuple fields.
 * @param outputColumns is a one-to-one mapping from field names to Kiji columns. Values from the
 *                      tuple fields will be written to their associated column.
 * @param rowRangeSpec is the specification for which interval of rows to scan.
 * @param rowFilterSpec is the specification for which row filter to apply.
 * @tparam T is the type of value from the source.
 */
sealed class TypedKijiSource[+T](
  val tableAddress: String,
  val timeRange: TimeRangeSpec,
  val inputColumns: List[ColumnInputSpec] = List(),
  val outputColumns: List[ColumnOutputSpec] = List(),
  val rowRangeSpec: RowRangeSpec,
  val rowFilterSpec: RowFilterSpec
  )(implicit conv: TupleConverter[T])
  extends Mappable[T] {

  import TypedKijiSource._

  override def converter[U >: T]: TupleConverter[U] = TupleConverter.asSuperConverter[T, U](conv)

  private val uri: KijiURI = KijiURI.newBuilder(tableAddress).build()


  /** A Kiji scheme intended to be used with Scalding/Cascading's hdfs mode. */
  val kijiTypedScheme: KijiTypedScheme =
    new KijiTypedScheme(
      tableAddress,
      timeRange,
      inputColumns,
      outputColumns,
      rowRangeSpec,
      rowFilterSpec)
  val typedLocalKijiScheme: TypedLocalKijiScheme =
    new TypedLocalKijiScheme(
      uri,
      timeRange,
      inputColumns,
      outputColumns,
      rowRangeSpec,
      rowFilterSpec
    )


  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      case Hdfs(_, _) => new KijiTap(uri, kijiTypedScheme).asInstanceOf[Tap[_, _, _]]
      case Test(buffers) => readOrWrite match {
        // Use Kiji's local tap and scheme when reading.
        case Read => {
          val scheme = typedLocalKijiScheme
          populateTestTable(
            uri,
            inputColumns,
            buffers(this),
            HBaseConfiguration.create())

          new LocalKijiTap(uri, scheme).asInstanceOf[Tap[_, _, _]]
        }
      }

      case _ => throw new RuntimeException("Trying to create invalid tap")
    }
  }

  override def toString: String =
    Objects
      .toStringHelper(this)
      .add("tableAddress", tableAddress)
      .add("timeRangeSpec", timeRange)
      .add("inputColumns", inputColumns)
      .add("outputColumns", outputColumns)
      .add("rowRangeSpec", rowRangeSpec)
      .add("rowFilterSpec", rowFilterSpec)
      .toString

  override def equals(obj: Any): Boolean = obj match {
    case other: TypedKijiSource[T] => (
      tableAddress == other.tableAddress
        && inputColumns == other.inputColumns
        && outputColumns == other.outputColumns
        && timeRange == other.timeRange
        && rowRangeSpec == other.rowRangeSpec
        && rowFilterSpec == other.rowFilterSpec)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hashCode(
      tableAddress,
      inputColumns,
      outputColumns,
      timeRange,
      rowRangeSpec,
      rowFilterSpec)
}


object TypedKijiSource {


  private[express] def newGetAllData(col: ColumnInputSpec): ColumnInputSpec = {
    ColumnInputSpec(
      col.columnName.toString,
      Integer.MAX_VALUE,
      col.filterSpec,
      col.pagingSpec,
      col.schemaSpec)
  }

  /**
   * Returns a map from field name to column input spec where the column input spec has been
   * configured as an output column.
   *
   * This is used in tests, when we use KijiScheme to read tuples from a Kiji table, and we want
   * to read all data in all of the columns, so the test can inspect all data in the table.
   *
   * @param columns to transform.
   * @return transformed map where the column input specs are configured for output.
   */
  private def inputColumnSpecifyAllData(
    columns: List[ColumnInputSpec]): List[ColumnInputSpec] = {
    columns.map(newGetAllData)
      // Need this to make the Map serializable (issue with mapValues)
      .map(identity)
  }

  // Test specific code below here.
  /**
   * Takes a buffer containing rows and writes them to the table at the specified uri.
   *
   * @param tableUri of the table to populate.
   * @param rows Tuples to write to populate the table with.
   * @param configuration defining the cluster to use.
   */
  private def populateTestTable(
    tableUri: KijiURI,
    inputColumns: List[ColumnInputSpec],
    rows: Option[Buffer[Tuple]],
    configuration: Configuration) {
    doAndRelease(Kiji.Factory.open(tableUri)) { kiji: Kiji =>
      // Layout to get the default reader schemas from.
      val layout = withKijiTable(tableUri, configuration) { table: KijiTable =>
        table.getLayout
      }

      val eidFactory = EntityIdFactory.getFactory(layout)

      // Write the desired rows to the table.
      withKijiTableWriter(tableUri, configuration) { writer: KijiTableWriter =>
        rows.toSeq.flatten.foreach { row: Tuple =>
          val tupleEntry = new TupleEntry(row)
          val entityId = tupleEntry
            .getObject(0).asInstanceOf[KijiRowData]
            .getEntityId.asInstanceOf[EntityId]

          // Write the timeline to the table.

          val rowData = tupleEntry
            .getObject(0).asInstanceOf[KijiRowData]

          inputColumns.foreach {
            inputColumnSpec: ColumnInputSpec =>
              rowData.getCells(inputColumnSpec.columnName.getFamily,
                inputColumnSpec.columnName.getQualifier).values().iterator().asScala.foreach {
                cell: KijiCell[_] => writer.put(entityId.toJavaEntityId(eidFactory),
                  inputColumnSpec.columnName.getFamily,
                  inputColumnSpec.columnName.getQualifier,
                  cell
                )
              }
          }
        }
      }
    }
  }


  /**
   * A LocalKijiScheme that loads rows in a table into the provided buffer. This class
   * should only be used during tests.
   *
   * @param buffer to fill with post-job table rows for tests.
   * @param timeRange of timestamps to read from each column.
   * @param inputColumns is a map of Scalding field name to ColumnInputSpec.
   * @param outputColumns is a map of ColumnOutputSpec to Scalding field name.
   * @param rowRangeSpec is the specification for which interval of rows to scan.
   * @param rowFilterSpec is the specification for which row filter to apply.
   */
  private class TestTypedLocalKijiScheme(
    val buffer: Option[mutable.Buffer[Tuple]],
    uri: KijiURI,
    timeRange: TimeRangeSpec,
    inputColumns: List[ColumnInputSpec],
    outputColumns: List[ColumnOutputSpec],
    rowRangeSpec: RowRangeSpec,
    rowFilterSpec: RowFilterSpec)
    extends TypedLocalKijiScheme(
      uri,
      timeRange,
      inputColumnSpecifyAllData(inputColumns),
      outputColumns,
      rowRangeSpec,
      rowFilterSpec) {
    override def sinkCleanup(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]) {
      // flush table writer
      sinkCall.getContext.writer.flush()
      // Store the output table.
      val conf: JobConf =
        HadoopUtil.createJobConf(process.getConfigCopy, new JobConf(HBaseConfiguration.create()))

      // Read table into buffer.
      withKijiTable(uri, conf) { table: KijiTable =>
        // We also want the entire time range, so the test can inspect all data in the table.
        val request: KijiDataRequest =
          BaseKijiScheme.buildRequest(table.getLayout, TimeRangeSpec.All, inputColumns)

        doAndClose(BaseLocalKijiScheme.openReaderWithOverrides(table, request)) { reader =>
          // Set up scanning options.
          val eidFactory = EntityIdFactory.getFactory(table.getLayout)
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
          doAndClose(reader.getScanner(request, scannerOptions)) { scanner: KijiRowScanner =>
            scanner.iterator().asScala.foreach { row: KijiRowData =>
              val tuple = KijiTypedScheme.rowToTuple(row)

              val newTupleValues = tuple
                .iterator()
                .asScala
                .map {
                // This converts stream into a list to force the stream to compute all of the
                // transformations that have been applied lazily to it. This is necessary
                // because some of the transformations applied in KijiScheme#rowToTuple have
                // dependencies on an open connection to a schema table.
                case stream: Stream[_] => stream.toList
                case x => x
              }.toSeq

              buffer.foreach {_ += new Tuple(newTupleValues: _*)}
            }
          }
        }
      }
      super.sinkCleanup(process, sinkCall)
    }
  }

}