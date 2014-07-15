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

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.kiji.express.KijiSuite
import org.kiji.schema.layout.{KijiTableLayouts, KijiTableLayout}
import org.kiji.express.flow.util.ResourceUtil
import org.kiji.schema.KijiTable
import com.twitter.scalding.{JobTest, TypedTsv, Args}
import scala.collection.mutable.Buffer
import org.apache.avro.util.Utf8


class AnagramCountJob(args: Args) extends KijiJob(args) {

  new TypedKijiSource[ExpressResult](
    args("input"),
    TimeRangeSpec.All,
    List(QualifiedColumnInputSpec.builder.withColumn("family", "column1").withMaxVersions(99).build)
  )
    .flatMap { entry: ExpressResult =>
    entry.cellsIterator[Utf8]("family", "column1").map {
      cell: FlowCell[Utf8] => cell.datum.toString.sorted
    }
  }.groupBy { word: String => word}.size.toTypedPipe
    .write(TypedTsv[(String, Long)](args("output")))
}


@RunWith(classOf[JUnitRunner])
class TypeSafeKijiSuite extends KijiSuite {

  val tableLayout: KijiTableLayout = ResourceUtil.layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)
  val kijiTable: KijiTable = makeTestKijiTable(tableLayout)

  test("A simple type safe job that counts the number of word anagrams.") {
    ResourceUtil.doAndRelease(kijiTable) {
      table: KijiTable =>

        val uri = table.getURI.toString
        val anagramCountInput = kijiRowDataSlice(
          kijiTable,
          "row01",
          "family:column1",
          (1L, "teba"), (2L, "alpah"), (3L, "alpha"),
          (4L, "beta"), (5L, "alaph"), (5L, "gamma"), (7L, "beta"))

        // Method to validate output.
        def validateOutput(outputBuffer: Buffer[(String, Long)]) {
          val bufferMap = outputBuffer.toMap
          assert(bufferMap("aagmm") == 1l)
          assert(bufferMap("aahlp") == 3l)
          assert(bufferMap("abet") == 2l)
        }

        JobTest(new AnagramCountJob(_))
          .arg("input", uri)
          .arg("output", "outputFile")
          .source(
            new TypedKijiSource[ExpressResult](
              uri,
              TimeRangeSpec.All,
              List(QualifiedColumnInputSpec.builder.withColumn("family", "column1").build)),
            anagramCountInput
          )
          .sink(TypedTsv[(String, Long)]("outputFile"))(validateOutput)
          // Run the test job.
          .run
          .finish
    }
  }
}


