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
  KijiInput.typedBuilder[ExpressResult]
    .withTableURI(args("input"))
    .withColumnSpecs(
      QualifiedColumnInputSpec.builder
      .withColumn("family", "column1")
      .withMaxVersions(all)
      .build
    ).build.flatMap{ entry: ExpressResult =>
      val expressColumn:ExpressColumn = entry.qualifiedColumn("family", "column1")
      expressColumn.values
        .map {
        e: Any  =>
          e.asInstanceOf[Utf8].toString.sorted
      }
    }.groupBy{word:String => word}.size.toTypedPipe
  .write(TypedTsv[(String, Long)](args("output")))
}

@RunWith(classOf[JUnitRunner])
class TypeSafeKijiSuite extends KijiSuite {

  val tableLayout: KijiTableLayout = ResourceUtil.layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)
  test("A simple type safe job that counts the number of word anagrams.") {
    val uri: String = ResourceUtil.doAndRelease(makeTestKijiTable(tableLayout)) {
      table: KijiTable => table.getURI.toString
    }

    /** Input tuples to use. */
    def anagramCountInput(): List[(EntityId, Seq[FlowCell[String]])] = {
      List((EntityId("row01"), slice("family:column1", (1L, "teba"), (2L, "alpah"), (3L, "alpha"),
        (4L, "beta"), (5L, "alaph"), (1L, "gamma"))))
    }

    def validateAnagramCount(outputBuffer: Buffer[(String, Long)]) {
      val outMap = outputBuffer.toMap
      assert(outMap.get("aagmm").get === 1l)
      assert(outMap.get("abet").get === 1l)
       assert(outMap.get("aahlp").get === 3l)
    }

    JobTest(new AnagramCountJob(_))
      .arg("input", uri)
      .arg("output", "outputFile")
      .source(
        KijiInput.TypedBuilder[ExpressResult]
          .withTableURI(uri)
          .withColumnSpecs(QualifiedColumnInputSpec.builder
          .withColumn("family", "column1")
          .withMaxVersions(all)
          .build)
          .build,
        anagramCountInput())
      .sink(TypedTsv[(String, Long)]("outputFile"))(validateAnagramCount)
      // Run the test job.
      .run
      .finish
  }
}
