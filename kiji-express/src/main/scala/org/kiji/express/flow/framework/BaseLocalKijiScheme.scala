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
package org.kiji.express.flow.framework

import java.io.OutputStream
import java.io.InputStream
import java.util.Properties

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import org.apache.hadoop.conf.Configuration

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiRowScanner
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.ColumnReaderSpec
import org.kiji.schema.KijiDataRequest.Column

/**
 * A Base trait containing Kiji-specific implementation of a Cascading `Scheme` that is common for
 * both the Fields API, and the Type-safe API for running locally. Scheme's [[LocalKijiScheme]] and
 * [[TypedLocalKijiScheme]] extend this trait and share the implemented methods.
 *
 * Note: [[LocalKijiScheme]] and [[TypedLocalKijiScheme]] log every row that was skipped because of
 * missing data in a column. It lacks the parameter `loggingInterval` in [[KijiScheme]] that
 * configures how many skipped rows will be logged.
 *
 * Note: Warnings about a missing serialVersionUID are ignored here. When KijiScheme is
 * serialized, the result is not persisted anywhere making serialVersionUID unnecessary.
 *
 * Note: If sourcing from a KijiTable, it is never closed.  The reason for this is that if any of
 * the columns in the request are paged, they might still need an open KijiTable for the rest of
 * the flow.  It is expected that any job using this as a source is not long-running and is
 * contained to a single JVM.
 */
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

/**
 * Companion object for the [[BaseLocalKijiScheme]].
 */
object BaseLocalKijiScheme {
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
      .map { column: Column => (column.getColumnName, column.getReaderSpec)}
      .toMap
    table.getReaderFactory.readerBuilder()
      .withColumnReaderSpecOverrides(overrides.asJava)
      .buildAndOpen()
  }
}

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
