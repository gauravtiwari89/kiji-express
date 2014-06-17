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

final class KijiCellEntry(private val eid: EntityId,
                               private val cells: Option[Seq[FlowCell[_]]],
                               private val transientCells: Option[TransientStream[FlowCell[_]]])
  extends Iterable[FlowCell[_]] {

  def entityId: EntityId = eid

  def mostRecentCell: FlowCell[_] =
    cells match {
      case Some(c) => c.head
      case None => transientCells.get.head
    }

  def iterator: Iterator[FlowCell[_]] = {
    cells match {
      case Some(c) => c.iterator
      case None => transientCells.get.iterator
    }
  }
}

object KijiCellEntry {

  def apply(te: TupleEntry): KijiCellEntry = {
    val entityId: EntityId = te.getTuple.getObject(0).asInstanceOf[EntityId]
    val flowCellIterable = te.getTuple.getObject(1)
    flowCellIterable match {
      case flowSeq: Seq[FlowCell[_]] =>
        new KijiCellEntry(entityId,
          Option(flowSeq), None)
      case _ =>
        new KijiCellEntry(entityId,
           None, Option(flowCellIterable.asInstanceOf[TransientStream[FlowCell[_]]]))
    }
  }
}
