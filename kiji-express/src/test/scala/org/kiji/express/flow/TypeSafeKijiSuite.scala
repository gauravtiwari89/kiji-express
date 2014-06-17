///**
// * (c) Copyright 2014 WibiData, Inc.
// *
// * See the NOTICE file distributed with this work for additional
// * information regarding copyright ownership.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.kiji.express.flow
//
//import org.scalatest.junit.JUnitRunner
//import org.junit.runner.RunWith
//import org.kiji.express.KijiSuite
//import org.kiji.schema.layout.{KijiTableLayouts, KijiTableLayout}
//import org.kiji.express.flow.util.ResourceUtil
//import org.kiji.schema.KijiTable
//import com.twitter.scalding.{JobTest, TypedTsv, Args}
//import scala.collection.mutable.Buffer
//
//
//class AnagramCountJob(args: Args) extends KijiJob(args) {
//  KijiInput.typedBuilder[(KijiCellEntry)]
//    .withTableURI(args("input"))
//    .withColumnSpecs(QualifiedColumnInputSpec.builder
//    .withColumn("family", "column1")
//    .withMaxVersions(all)
//    .withPagingSpec(PagingSpec.Cells(3))
//    .build -> 'word)
//    .build
//    .flatMap{ entry: KijiCellEntry => entry.map { f: FlowCell[_] => f.datum.toString.sorted}}
//    .group
//    .toTypedPipe
//  .write(TypedTsv[(String, String)](args("output")))
//}
//
//@RunWith(classOf[JUnitRunner])
//class TypeSafeKijiSuite extends KijiSuite {
//
//  val tableLayout: KijiTableLayout = ResourceUtil.layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)
//  test("A simple type safe job that counts the number of word anagrams.") {
//    val uri: String = ResourceUtil.doAndRelease(makeTestKijiTable(tableLayout)) {
//      table: KijiTable => table.getURI.toString
//    }
//
//    /** Input tuples to use. */
//    def anagramCountInput(): List[(EntityId, Seq[FlowCell[String]])] = {
//      List((EntityId("row01"), slice("family:column1", (1L, "teba"), (2L, "alpah"), (3L, "alpha"),
//        (4L, "beta"), (5L, "alaph"), (6L, "gamma"))))
//    }
//
//    def validateWordConcat(outputBuffer: Buffer[(String , String)]) {
//      println(outputBuffer)
//    }
//
//    JobTest(new AnagramCountJob(_))
//      .arg("input", uri)
//      .arg("output", "outputFile")
//      .source(
//        KijiInput.builder
//          .withTableURI(uri)
//          .withColumnSpecs(QualifiedColumnInputSpec.builder
//          .withColumn("family", "column1")
//          .withMaxVersions(all)
//          .withPagingSpec(PagingSpec.Cells(3))
//          .build -> 'word)
//          .build,
//        anagramCountInput())
//      .sink(TypedTsv[(String, String)]("outputFile"))(validateWordConcat)
//      // Run the test job.
//      .run
//      .finish
//  }
//}
