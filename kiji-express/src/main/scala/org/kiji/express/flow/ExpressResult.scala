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

import cascading.tuple.TupleEntry
import org.kiji.express.flow.framework.KijiScheme

final class ExpressResult(te: TupleEntry) {

  lazy val entityId: EntityId = te.getObject(KijiScheme.EntityIdField).asInstanceOf[EntityId]

  def qualifiedColumn(familyName: String, qualifier: String): ExpressColumn =
    ExpressColumn(
      te.getObject(List(familyName, qualifier).mkString(":")).asInstanceOf[Seq[FlowCell[_]]]
    )
}

case class ExpressColumn(flowCell: Seq[FlowCell[_]]) extends Iterable[FlowCell[_]] {

  def mostRecent: FlowCell[_] = flowCell.head

  def mostRecentValue :Any = mostRecent.datum

  def iterator: Iterator[FlowCell[_]] = flowCell.iterator

  def values: Iterator[_] = flowCell.iterator.map{cell: FlowCell[_]=> cell.datum}
}


object ExpressResult {
  def apply(te: TupleEntry): ExpressResult = new ExpressResult(te)
}
