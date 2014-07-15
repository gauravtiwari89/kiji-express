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

import scala.collection.JavaConversions.asScalaIterator

import org.kiji.schema.KijiCell
import org.kiji.schema.KijiRowData

/**
 * A wrapper class around [[org.kiji.schema.KijiRowData]] that contains methods to retrieve and
 * iterate through the data. This class is used as the type for
 * [[com.twitter.scalding.typed.TypedPipe]] received from [[org.kiji.express.flow.TypedKijiSource]]
 * for the type safe API.
 *
 * @param row of the requested data from the Kiji table.
 */
final class ExpressResult(row: KijiRowData) {

  /**
   * Fetch the [[EntityId]] for the row.
   *
   * @return the entityId for the row.
   */
  def entityId: EntityId = EntityId.fromJavaEntityId(row.getEntityId)

  /**
   * Fetch the most recent cell for a  qualified column.
   *
   * @tparam T is the type of the datum contained in [[FlowCell]].
   * @return the [[FlowCell]] containing the most recent cell.
   */
  def mostRecentCell[T](family: String, qualifier: String): FlowCell[T] = {
    FlowCell(row.getMostRecentCell(family, qualifier))
  }

  /**
   * Fetch a cell with a specific timestamp.
   *
   * @param timestamp is timestamp associated with the requested cell.
   * @tparam T is the type of the datum contained in [[FlowCell]]
   * @return a [[FlowCell]] that contains the requested cell.
   */
  def cell[T](family: String, qualifier: String, timestamp: Long): FlowCell[T] = {
    FlowCell(row.getCell(family, qualifier, timestamp))
  }

  /**
   * Fetch an iterator for requested column.
   *
   * @tparam T is the type of the datum that will be contained in [[FlowCell]]
   * @return an iterator of [[FlowCell]] for the column requested.
   */
  def cellsIterator[T](family: String, qualifier: String): Iterator[FlowCell[T]] = {
    ExpressResultIterator[T](row.iterator[T](family, qualifier))
  }

  /**
   * Fetch an iterator for the requested family.
   *
   * @param family of the column requested.
   * @tparam T is the type of the datum that will be contained in [[FlowCell]]
   * @return an iterator of [[FlowCell]]'s for the family requested.
   */
  def familyIterator[T](family: String): Iterator[FlowCell[T]] = {
    ExpressResultIterator[T](row.iterator[T](family))
  }
}

/**
 * Wrapper around the iterators received from [[KijiRowData]] to simplify the java <-> scala
 * conversions from a [[KijiCell]] to [[FlowCell]].
 *
 * @param resultIterator is the iterator from [[KijiRowData]] methods.
 * @tparam T is the type of the datum contained in [[KijiCell]].
 */
final private[express] case class ExpressResultIterator[T](resultIterator: Iterator[KijiCell[T]])
  extends Iterator[FlowCell[T]] {

  /** @inheritdoc*/
  override def hasNext: Boolean = resultIterator.hasNext

  /** @inheritdoc*/
  override def next(): FlowCell[T] = FlowCell(resultIterator.next())
}

/**
 * Companion object for the ExpressResult class.
 */
object ExpressResult {
  /**
   * Creates and returns an instance of ExpressResult.
   *
   * @param rowData is a row of requested data from Kiji Table.
   * @return an instance of ExpressResult for the specified row.
   */
  def apply(rowData: KijiRowData): ExpressResult = new ExpressResult(rowData)
}


