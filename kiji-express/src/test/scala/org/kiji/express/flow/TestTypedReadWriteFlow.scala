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

import org.kiji.schema.layout.{KijiTableLayouts, KijiTableLayout}
import org.kiji.express.flow.util.ResourceUtil
import org.kiji.schema._
import org.kiji.express.KijiSuite
import org.kiji.schema.util.InstanceBuilder
import com.twitter.scalding._
import org.junit.Assert
import org.apache.avro.util.Utf8
import com.twitter.scalding.Local
import scala.Some



class TestTypedReadWriteFlow extends KijiSuite{

  test("A simple job that reads and writes to a table using Type safe API."){
    val tableLayout: KijiTableLayout = ResourceUtil.layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)
    val kijiTable: KijiTable = makeTestKijiTable(tableLayout)

    def validateWritesToTable(
        row:String,
        family:String,
        columns:List[String],
        table:KijiTable
    ):Boolean = {
      ResourceUtil.doAndClose(table.openTableReader()) { reader =>
        val rowData: KijiRowData = reader.get(
            table.getEntityId(row),
            KijiDataRequest.create(family))
        columns.foreach {
          qualifier: String =>
            //Assert non-empty for cells read from the table.
            Assert.assertFalse(rowData.getCells(family, qualifier).isEmpty())
        }
        true
      }
    }

    ResourceUtil.doAndRelease(Kiji.Factory.open(kijiTable.getURI)) {
      kiji =>
        ResourceUtil.doAndRelease(kiji.openTable(tableLayout.getName)) { table =>
          new InstanceBuilder(kiji)
              .withTable(table)
              .withRow("row1")
                .withFamily("family")
                  .withQualifier("column1").withValue("v1")
                  .withQualifier("column2").withValue("v2")
          .build()

          class SimpleReadWriteJob(args:Args) extends KijiJob(args){
            KijiInput.typedBuilder
                .withTableURI(table.getURI.toString)
                .withColumnSpecs(
                    List(
                        QualifiedColumnInputSpec
                          .builder
                          .withColumn("family", "column1")
                          .build,
                        QualifiedColumnInputSpec
                          .builder
                          .withColumn("family", "column2")
                          .build)
              ).build
            .map{ row:ExpressResult =>
              val mostRecent1=  row.mostRecentCell[Utf8]("family", "column1")
              val col1 = ExpressColumnOutput(
                    EntityId("row2"),
                    "family",
                    "column1",
                    mostRecent1.datum.toString,
                    timeStamp=Some(mostRecent1.version))
              val mostRecent2=  row.mostRecentCell[Utf8]("family", "column2")
              val col2 = ExpressColumnOutput(
                    EntityId("row2"),
                    "family",
                    "column2",
                    mostRecent2.datum.toString,
                    timeStamp=Some(mostRecent2.version))
              (col1, col2)
            }
            .write(KijiOutput.typedSinkForTable(table.getURI))
          }

          val args = Mode.putMode(Local(strictSources = true), Args(List()))
          Assert.assertTrue(new SimpleReadWriteJob(args).run)
          //Validate Writes.
          validateWritesToTable("row2", "family", List("column1", "column2"), table)
        }
    }
  }
}
