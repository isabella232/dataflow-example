/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.example.dataflow;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Sets;
import com.google.api.services.dataflow.Dataflow;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.pubsub.spi.v1.TopicAdminClient;
import com.google.pubsub.v1.TopicName;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.PipelineResult;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * The utility class that sets up and tears down external resources, starts the Google Cloud Pub/Sub
 * injector, and cancels the streaming and the injector pipelines once the program terminates.
 *
 * <p>It is used to run Dataflow examples, such as TrafficMaxLaneFlow and TrafficRoutes.
 */
public class DataflowExampleUtils {

  private final DataflowPipelineOptions options;
  private Dataflow dataflowClient = null;
  private Set<DataflowPipelineJob> jobsToCancel = Sets.newHashSet();
  private List<String> pendingMessages = Lists.newArrayList();

  /** Define an interface that supports the PubSub and BigQuery example options. */
  public interface DataflowExampleUtilsOptions
      extends DataflowExampleOptions, ExamplePubsubTopicOptions, ExampleBigQueryTableOptions {}

  DataflowExampleUtils(DataflowPipelineOptions options) {
    this.options = options;
  }

  /** Do resources and runner options setup. */
  public DataflowExampleUtils(DataflowPipelineOptions options, boolean isUnbounded)
      throws Exception {
    this.options = options;
    setupResourcesAndRunner(isUnbounded);
  }

  /**
   * Sets up external resources that are required by the example, such as Pub/Sub topics and
   * BigQuery tables.
   *
   * @throws Exception if there is a problem setting up the resources
   */
  void setup() throws Exception {
    setupPubsubTopic();
    setupBigQueryTable();
  }

  private void tearDown() {
  }

  /** Set up external resources, and configure the runner appropriately. */
  private void setupResourcesAndRunner(boolean isUnbounded) throws Exception {
    if (isUnbounded) {
      options.setStreaming(true);
    }
    setup();
    setupRunner();
  }

  /**
   * Sets up the Google Cloud Pub/Sub topic.
   *
   * <p>If the topic doesn't exist, a new topic with the given name will be created.
   *
   * @throws IOException if there is a problem setting up the Pub/Sub topic
   */
  private void setupPubsubTopic() throws Exception {
    ExamplePubsubTopicOptions pubsubTopicOptions = options.as(ExamplePubsubTopicOptions.class);
    if (!pubsubTopicOptions.getPubsubTopic().isEmpty()) {
      pendingMessages.add("*******************Set Up Pubsub Topic*********************");
      setupPubsubTopic(pubsubTopicOptions.getPubsubTopic());
      pendingMessages.add(
          "The Pub/Sub topic has been set up for this example: "
              + pubsubTopicOptions.getPubsubTopic());
    }
  }

  /**
   * Sets up the BigQuery table with the given schema.
   *
   * <p>If the table already exists, the schema has to match the given one. Otherwise, the example
   * will throw a RuntimeException. If the table doesn't exist, a new table with the given schema
   * will be created.
   */
  private void setupBigQueryTable() {
    ExampleBigQueryTableOptions bigQueryTableOptions =
        options.as(ExampleBigQueryTableOptions.class);

    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    String projectName = bigQueryTableOptions.getProject();
    String datasetName = bigQueryTableOptions.getBigQueryDataset();
    String tableName = bigQueryTableOptions.getBigQueryTable();

    Schema schema = Schema.newBuilder()
        .addField(Field.of("station_id", Field.Type.string()))
        .addField(Field.of("direction", Field.Type.string()))
        .addField(Field.of("freeway", Field.Type.string()))
        .addField(Field.of("lane_max_flow", Field.Type.integer()))
        .addField(Field.of("lane", Field.Type.string()))
        .addField(Field.of("avg_occ", Field.Type.floatingPoint()))
        .addField(Field.of("avg_speed", Field.Type.floatingPoint()))
        .addField(Field.of("total_flow", Field.Type.integer()))
        .addField(Field.of("window_timestamp", Field.Type.timestamp()))
        .addField(Field.of("recorded_timestamp", Field.Type.string()))
        .build();

    // Prepares a new dataset
    Dataset dataset = null;
    Table table = null;

    DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();

    TableInfo tableInfo = TableInfo
        .newBuilder(TableId.of(projectName, datasetName, tableName), StandardTableDefinition.of(schema))
        .build();

    // Creates the dataset
    try {
      dataset = bigquery.create(datasetInfo);
      pendingMessages.add(dataset.getDatasetId().getDataset() + " created.");
    } catch(Exception e) {
      System.err.println(e.getMessage());
    }

    try {
      table = bigquery.create(tableInfo);
      pendingMessages.add(table.getTableId().getTable() + " created.");
    } catch(Exception e) {
      System.err.println(e.getMessage());
    }


  }

  private void setupPubsubTopic(String topicId) throws Exception {
    String projectId = ServiceOptions.getDefaultProjectId();
    TopicName topic = TopicName.create(projectId, topicId);

    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      topicAdminClient.createTopic(topic);
    } catch (Exception e) {
      System.err.print(e.getMessage());
      // throw e;
    }
  }

  /**
   * Do some runner setup: check that the DirectPipelineRunner is not used in conjunction with
   * streaming, and if streaming is specified, use the DataflowPipelineRunner. Return the streaming
   * flag value.
   */
  private void setupRunner() {
    if (options.isStreaming()) {
      /*
      if (options.getRunner() == DirectRunner.class) {
        throw new IllegalArgumentException(
            "Processing of unbounded input sources is not supported with the DirectRunner.");
      }
      */
      // In order to cancel the pipelines automatically,
      // {@literal DataflowRunner} is forced to be used.
      options.setRunner(DataflowRunner.class);
    }
  }

  /**
   * If {@literal DataflowPipelineRunner} or {@literal BlockingDataflowPipelineRunner} is used,
   * waits for the pipeline to finish and cancels it (and the injector) before the program exists.
   */
  void waitToFinish(PipelineResult result) {
    if (result instanceof DataflowPipelineJob) {
      final DataflowPipelineJob job = (DataflowPipelineJob) result;
      jobsToCancel.add(job);
      if (!options.as(DataflowExampleOptions.class).getKeepJobsRunning()) {
        addShutdownHook(jobsToCancel);
      }
      try {
        job.waitUntilFinish(Duration.millis(-1L));
      } catch (Exception e) {
        throw new RuntimeException("Failed to wait for job to finish: " + job.getJobId());
      }
    } else {
      // Do nothing if the given PipelineResult doesn't support waitToFinish(),
      // such as EvaluationResults returned by DirectPipelineRunner.
    }
  }

  private void addShutdownHook(final Collection<DataflowPipelineJob> jobs) {
    if (dataflowClient == null) {
      dataflowClient = options.getDataflowClient();
    }

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                tearDown();
                printPendingMessages();
                for (DataflowPipelineJob job : jobs) {
                  System.out.println("Canceling example pipeline: " + job.getJobId());
                  try {
                    job.cancel();
                  } catch (IOException e) {
                    System.out.println(
                        "Failed to cancel the job,"
                            + " please go to the Developers Console to cancel it manually");
                    System.out.println(
                        MonitoringUtil.getJobMonitoringPageURL(job.getProjectId(), job.getJobId()));
                  }
                }

                for (DataflowPipelineJob job : jobs) {
                  boolean cancellationVerified = false;
                  for (int retryAttempts = 6; retryAttempts > 0; retryAttempts--) {
                    if (job.getState().isTerminal()) {
                      cancellationVerified = true;
                      System.out.println("Canceled example pipeline: " + job.getJobId());
                      break;
                    } else {
                      System.out.println(
                          "The example pipeline is still running. Verifying the cancellation.");
                    }
                    try {
                      Thread.sleep(10000);
                    } catch (InterruptedException e) {
                      // Ignore
                    }
                  }
                  if (!cancellationVerified) {
                    System.out.println(
                        "Failed to verify the cancellation for job: " + job.getJobId());
                    System.out.println("Please go to the Developers Console to verify manually:");
                    System.out.println(
                        MonitoringUtil.getJobMonitoringPageURL(job.getProjectId(), job.getJobId()));
                  }
                }
              }
            });
  }

  private void printPendingMessages() {
    System.out.println();
    System.out.println("***********************************************************");
    System.out.println("***********************************************************");
    for (String message : pendingMessages) {
      System.out.println(message);
    }
    System.out.println("***********************************************************");
    System.out.println("***********************************************************");
  }
}
