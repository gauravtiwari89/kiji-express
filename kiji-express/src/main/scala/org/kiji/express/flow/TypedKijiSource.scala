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
package org.kiji.express.flow


import cascading.tap.Tap

import com.google.common.base.Objects

import com.twitter.scalding.AccessMode
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Mappable
import com.twitter.scalding.Mode
import com.twitter.scalding.Local
import com.twitter.scalding.Test
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import com.twitter.scalding.typed.TypedSink

import org.kiji.schema.KijiURI
import org.kiji.express.flow.framework.KijiTap
import org.kiji.express.flow.framework.LocalKijiTap
import org.kiji.express.flow.framework.TypedKijiScheme
import org.kiji.express.flow.framework.TypedLocalKijiScheme

/**
 * TypedKijiSource is a type safe representation of [[KijiSource]]. This class extends the
 * [[Mappable]] trait in scalding to allow compile time type checking.
 *
 * When reading from a Kiji table, a `TypedKijiSource` will provide a view of the KijiTable as a
 * collection of tuples that correspond to rows from the Kiji Table. The columns that need to be
 * read can be configured along with the time spans that cells retrieved must belong to.
 * Each retrieved row is wrapped in a [[ExpressResult]] object which contains methods to
 * allow access to the row data.
 *
 * When writing to a Kiji table, a `TypedKijiSource` expects the value in the TypedPipe to be
 * either the type [[ExpressColumnOutput]] or a scalding Tuple of multiple[[ExpressColumnOutput]].
 * The [[ExpressColumnOutput]] object contains the [[EntityId]] and other identifying information
 * which is used to determine where the data is stored in the KijiTable.
 *
 * [[TypedKijiSource]] extends both [[Mappable]] and [[TypedSink]] as it is used as both, the
 * source and the sink. [[Mappable]] requires the type parameter T to be present in the co-variant
 * position for the TupleConverter and [[TypedSink]] requires it to be present as contra-variant
 * for the TupleSetter. But since we force a type of [[ExpressResult]] for reads from the Kiji table
 * we require the type parameter to exist only for the TupleSetter. Hence, the type here is
 * specified in a contra-variant position.
 *
 * End-users cannot directly obtain instances of `TypedKijiSource`. Instead,
 * they should use the factory methods provided as part of the [[org.kiji.express.flow]] module.
 *
 * @param tableAddress is a Kiji URI addressing the Kiji table to read or write to.
 * @param timeRange that cells read must belong to. Ignored when the source is used to write.
 * @param inputColumns is a one-to-one mapping from field names to Kiji columns. The columns in the
 *                     map will be read into their associated tuple fields.
 * @param rowRangeSpec is the specification for which interval of rows to scan.
 * @param rowFilterSpec is the specification for which row filter to apply.
 * @param conv is the tuple converter definition passed in implicitly.
 * @param tset is the tuple setter definition passed in implicitly.
 * @tparam T is the type of value from the source.
 */
final class TypedKijiSource[-T](
    val tableAddress: String,
    val timeRange: TimeRangeSpec,
    val inputColumns: List[ColumnInputSpec] = List(),
    val rowRangeSpec: RowRangeSpec = RowRangeSpec.All,
    val rowFilterSpec: RowFilterSpec = RowFilterSpec.NoFilter
    )(implicit conv: TupleConverter[ExpressResult], tset: TupleSetter[T])
    extends Mappable[ExpressResult] with TypedSink[T] {

  /**
   * Default implementation of a converter method that returns a [[TupleConverter]] for the super
    * type of [[ExpressResult]].
    *
    * @tparam U is the type parameter for the [[TupleConverter]] returned.
    * @return the [[TupleConverter]] object with the new type.
    */
  override def converter[U >: ExpressResult]: TupleConverter[U] =
      TupleConverter.asSuperConverter[ExpressResult, U](conv)

  /**
   * Default implementation of a converter method that returns a [[TupleSetter]] for the subtype of
   * type T
   * @tparam U is the type parameter for the [[TupleSetter]] returned.
   * @return the [[TupleSetter]] object with the new type.
   */
  override def setter[U <: T]: TupleSetter[U] = TupleSetter.asSubSetter[T, U](tset)

  private val uri: KijiURI = KijiURI.newBuilder(tableAddress).build()


  /** A Typed Kiji scheme intended to be used with Scalding/Cascading's hdfs mode. */
  val typedKijiScheme: TypedKijiScheme =
      new TypedKijiScheme(
          tableAddress,
          timeRange,
          inputColumns,
          rowRangeSpec,
          rowFilterSpec)

  /** A Typed Local Kiji scheme intended to be used with Scalding/Cascading's local mode. */
  val typedLocalKijiScheme: TypedLocalKijiScheme =
      new TypedLocalKijiScheme(
          uri,
          timeRange,
          inputColumns,
          rowRangeSpec,
          rowFilterSpec)

  /**
   * Create a connection to the physical data source (also known as a Tap in Cascading)
   * which, in this case, is a [[org.kiji.schema.KijiTable]].
   *
   * @param readOrWrite Specifies if this source is to be used for reading or writing.
   * @param mode Specifies which job runner/flow planner is being used.
   * @return A tap to use for this data source.
   */
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      case Hdfs(_, _) => new KijiTap(uri, typedKijiScheme).asInstanceOf[Tap[_, _, _]]
      case Local(_) => new LocalKijiTap(uri, typedLocalKijiScheme).asInstanceOf[Tap[_, _, _]]
      case Test(_) => new LocalKijiTap(uri, typedLocalKijiScheme).asInstanceOf[Tap[_, _, _]]
      case _ => throw new RuntimeException("Trying to create invalid tap")
    }
  }

  override def toString: String =
    Objects
        .toStringHelper(this)
        .add("tableAddress", tableAddress)
        .add("timeRangeSpec", timeRange)
        .add("inputColumns", inputColumns)
        .add("rowRangeSpec", rowRangeSpec)
        .add("rowFilterSpec", rowFilterSpec)
        .toString

  override def equals(obj: Any): Boolean = obj match {
    case other: TypedKijiSource[T] => (
      tableAddress == other.tableAddress
          && inputColumns == other.inputColumns
          && timeRange == other.timeRange
          && rowRangeSpec == other.rowRangeSpec
          && rowFilterSpec == other.rowFilterSpec)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hashCode(
        tableAddress,
        inputColumns,
        timeRange,
        rowRangeSpec,
        rowFilterSpec)
}
