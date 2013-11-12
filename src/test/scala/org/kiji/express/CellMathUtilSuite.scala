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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import org.kiji.express.util.CellMathUtil

@RunWith(classOf[JUnitRunner])
class CellMathUtilSuite
    extends FunSuite
    with ShouldMatchers {
  val mapCell0: Cell[Long] = Cell("info", "a", 0L, 0L)
  val mapCell1: Cell[Long] = Cell("info", "a", 1L, 1L)
  val mapCell2: Cell[Long] = Cell("info", "b", 2L, 0L)
  val mapCell3: Cell[Long] = Cell("info", "b", 3L, 3L)
  val mapCell4: Cell[Long] = Cell("info", "c", 4L, 0L)
  val mapCellSeq: List[Cell[Long]] = List(
      mapCell4,
      mapCell3,
      mapCell2,
      mapCell1,
      mapCell0
  )

  test("CellMathUtils should properly sum simple types") {
    assert(4 === CellMathUtil.sum(mapCellSeq))
  }

  test("CellMathUtils should properly compute the squared sum of simple types") {
    assert(10.0 === CellMathUtil.sumSquares(mapCellSeq))
  }

  test("CellMathUtils should properly compute the average of simple types") {
    assert(0.8 === CellMathUtil.mean(mapCellSeq))
  }

  test("CellMathUtils should properly compute the standard deviation of simple types") {
    CellMathUtil.stddev(mapCellSeq) should be (1.16619 plusOrMinus 0.1)
  }

  test("CellMathUtils should properly compute the variance of simple types") {
    CellMathUtil.variance(mapCellSeq) should be (1.36 plusOrMinus 0.1)
  }

  test("CellMathUtils should properly find the minimum of simple types") {
    assert(0.0 === CellMathUtil.min(mapCellSeq))
  }

  test("CellMathUtils should properly find the maximum of simple types") {
    assert(3.0 === CellMathUtil.max(mapCellSeq))
  }
}