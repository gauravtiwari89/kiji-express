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
package org.kiji.express.flow.framework

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.kiji.annotations.{ApiStability, ApiAudience}
import org.kiji.schema._
import org.apache.hadoop.conf.Configuration
import cascading.scheme.{SinkCall, SourceCall, Scheme}
import java.util.Properties
import cascading.flow.FlowProcess
import cascading.tap.Tap
import java.io.{OutputStream, InputStream}
import org.kiji.schema.layout.ColumnReaderSpec
import org.kiji.schema.KijiDataRequest.Column

/**
 * Encapsulates the state required to read from a Kiji table locally, for use in
 * [[org.kiji.express.flow.framework.LocalKijiScheme]].
 *
 * @param reader that has an open connection to the desired Kiji table.
 * @param scanner that has an open connection to the desired Kiji table.
 * @param iterator that maintains the current row pointer.
 * @param tableUri of the kiji table.
 */
@ApiAudience.Private
@ApiStability.Stable
final private[express] case class InputContext(
  reader: KijiTableReader,
  scanner: KijiRowScanner,
  iterator: Iterator[KijiRowData],
  tableUri: KijiURI,
  configuration: Configuration
  )

trait BaseLocalKijiScheme
  extends Scheme[Properties, InputStream, OutputStream, InputContext, DirectKijiSinkContext] {

  /**
   * Sets any configuration options that are required for running a local job
   * that reads from a Kiji table.
   *
   * @param process flow being built.
   * @param tap that is being used with this scheme.
   * @param conf is an unused Properties object that is a stand-in for a job configuration object.
   */
  override def sourceConfInit(
    process: FlowProcess[Properties],
    tap: Tap[Properties, InputStream, OutputStream],
    conf: Properties) {
    // No-op. Setting options in a java Properties object is not going to help us read from
    // a Kiji table.
  }

  /**
   * Cleans up any resources used to read from a Kiji table.
   *
   * Note: This does not close the KijiTable!  If one of the columns of the request was paged,
   * it will potentially still need access to the Kiji table even after the tuples have been
   * sourced.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   */
  override def sourceCleanup(
    process: FlowProcess[Properties],
    sourceCall: SourceCall[InputContext, InputStream]) {
    // Set the context to null so that we no longer hold any references to it.
    sourceCall.setContext(null)
  }


  /**
   * Sets any configuration options that are required for running a local job
   * that writes to a Kiji table.
   *
   * @param process Current Cascading flow being built.
   * @param tap The tap that is being used with this scheme.
   * @param conf The job configuration object.
   */
  override def sinkConfInit(
    process: FlowProcess[Properties],
    tap: Tap[Properties, InputStream, OutputStream],
    conf: Properties) {
    // No-op. Setting options in a java Properties object is not going to help us write to
    // a Kiji table.
  }

  /**
   * Cleans up any resources used to write to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkCleanup(
    process: FlowProcess[Properties],
    sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]) {
    val writer = sinkCall.getContext.writer
    writer.flush()
    writer.close()
    sinkCall.setContext(null)
  }

}

object BaseLocalKijiScheme{

  /**
   * Opens a Kiji table reader correctly specifying column schema overrides from a KijiDataRequest.
   *
   * @param table to use to open a reader.
   * @param request for data from the target Kiji table.
   * @return a Kiji table reader. Close this reader when it is no longer needed.
   */
  def openReaderWithOverrides(table: KijiTable, request: KijiDataRequest): KijiTableReader = {
    val overrides: Map[KijiColumnName, ColumnReaderSpec] = request
      .getColumns
      .asScala
      .map { column: Column => (column.getColumnName, column.getReaderSpec) }
      .toMap
    table.getReaderFactory.readerBuilder()
      .withColumnReaderSpecOverrides(overrides.asJava)
      .buildAndOpen()
  }

}
