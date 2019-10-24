/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.ngram;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test for NGramTransform plugin.
 */
public class NGramTransformTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", "4.0.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "4.0.0");
  private static final String APP_NAME = "NGramTransformTest";
  private static final String OUTPUT_FIELD = "ngrams";
  private static final String FIELD_TO_BE_TRANSFORMED = "tokens";
  private static final String ADDITIONAL_FIELD = "name";
  private static final String[] DATA_TO_BE_TRANSFORMED1 = {"hi", "i", "am", "cdap"};
  private static final String[] DATA_TO_BE_TRANSFORMED2 = {"how", "are", "you", "cdap"};
  private static final String[] DATA_TO_BE_TRANSFORMED3 = {"hi", "i"};
  private static final String SINGLE_FIELD_DATASET = "SingleField";

  private static final Schema SOURCE_SCHEMA_SINGLE = Schema.recordOf("sourceRecord",
                                                                     Schema.Field.of(FIELD_TO_BE_TRANSFORMED,
                                                                     Schema.arrayOf(Schema.of(Schema.Type.STRING)))
  );

  private static final Schema SOURCE_SCHEMA_SINGLE_NEGATIVE_TEST = Schema.recordOf("sourceRecord",
                                                                               Schema.Field.of(FIELD_TO_BE_TRANSFORMED,
                                                                               Schema.of(Schema.Type.STRING))
  );
  private static final Schema SOURCE_SCHEMA_MULTIPLE = Schema.recordOf("sourceRecord",
                                                                       Schema.Field.of(ADDITIONAL_FIELD,
                                                                       Schema.of(Schema.Type.STRING)),
                                                                       Schema.Field.of(FIELD_TO_BE_TRANSFORMED,
                                                                       Schema.arrayOf(Schema.of(Schema.Type.STRING)))
  );

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);
    // add artifact for spark plugins
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true)
    );
    addPluginArtifact(NamespaceId.DEFAULT.artifact("ngram-transform-plugin", "1.0.0"), parents,
                      NGramTransform.class);
  }

  /**
   * @param mockNameOfSourcePlugin used while adding ETLStage for mock source
   * @param mockNameOfSinkPlugin   used while adding ETLStage for mock sink
   * @param ngramSize              size of NGram
   * @return ETLBatchConfig
   */
  private ETLBatchConfig buildETLBatchConfig(String mockNameOfSourcePlugin,
                                             String mockNameOfSinkPlugin, String ngramSize) {
    return ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(mockNameOfSourcePlugin)))
      .addStage(new ETLStage("sparkcompute",
                             new ETLPlugin(NGramTransform.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("outputField", OUTPUT_FIELD,
                                                           "fieldToBeTransformed", FIELD_TO_BE_TRANSFORMED,
                                                           "ngramSize", ngramSize),
                                           null))).addStage(new ETLStage("sink",
                                                                         MockSink.getPlugin(mockNameOfSinkPlugin)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();
  }

  @Test
  public void testMultiFieldsSourceWith2N() throws Exception {
    String mockSource = "Multiple";
    String multiFieldData = "MultipleFields";
    /*
     * source --> sparkcompute --> sink
    */
    ETLBatchConfig etlConfig = buildETLBatchConfig(mockSource, multiFieldData, "2");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(APP_NAME);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    DataSetManager<Table> inputManager = getDataset(mockSource);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(FIELD_TO_BE_TRANSFORMED,
                                                           DATA_TO_BE_TRANSFORMED1)
                                                      .set(ADDITIONAL_FIELD, "CDAP1").build(),
      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(FIELD_TO_BE_TRANSFORMED,
                                                           DATA_TO_BE_TRANSFORMED2)
                                                      .set(ADDITIONAL_FIELD, "CDAP2").build()
    );
    MockSource.writeInput(inputManager, input);
    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    DataSetManager<Table> nGrams = getDataset(multiFieldData);
    List<StructuredRecord> output = MockSink.readOutput(nGrams);
    Set<List> results = new HashSet<>();
    for (StructuredRecord structuredRecord : output) {
      results.add((ArrayList) structuredRecord.get(OUTPUT_FIELD));
    }
    Assert.assertEquals(2, output.size());
    Assert.assertEquals(getExpectedData(), results);
    StructuredRecord row = output.get(0);
    Assert.assertEquals(1, row.getSchema().getFields().size());
    Assert.assertEquals("ARRAY", row.getSchema().getField(OUTPUT_FIELD).getSchema().getType().toString());
  }

  @Test
  public void testSingleFieldSourceWith3N() throws Exception {
    String mockSource = "Single";
    /*
     * source --> sparkcompute --> sink
    */
    ETLBatchConfig etlConfig = buildETLBatchConfig(mockSource, SINGLE_FIELD_DATASET, "3");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(APP_NAME);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    DataSetManager<Table> inputManager = getDataset(mockSource);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set(FIELD_TO_BE_TRANSFORMED,
                                                         DATA_TO_BE_TRANSFORMED1).build(),
      StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set(FIELD_TO_BE_TRANSFORMED,
                                                         DATA_TO_BE_TRANSFORMED2).build()
    );
    MockSource.writeInput(inputManager, input);
    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    DataSetManager<Table> tokenizedTextSingle = getDataset(SINGLE_FIELD_DATASET);
    List<StructuredRecord> output = MockSink.readOutput(tokenizedTextSingle);
    Set<List> results = new HashSet<>();
    for (StructuredRecord structuredRecord : output) {
      results.add((ArrayList) structuredRecord.get(OUTPUT_FIELD));
    }
    Assert.assertEquals(getExpectedDataFor3N(), results);
    Assert.assertEquals("ARRAY", output.get(0).getSchema().getField(OUTPUT_FIELD).
      getSchema().getType().toString());
  }

  @Test
  public void testFewerSequenceThanNStrings() throws Exception {
    String mockSource = "Less";
    /*
     * source --> sparkcompute --> sink
    */
    ETLBatchConfig etlConfig = buildETLBatchConfig(mockSource, SINGLE_FIELD_DATASET, "3");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(APP_NAME);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    DataSetManager<Table> inputManager = getDataset(mockSource);
    List<StructuredRecord> input = ImmutableList.of(StructuredRecord.builder(SOURCE_SCHEMA_SINGLE)
                                                .set(FIELD_TO_BE_TRANSFORMED, DATA_TO_BE_TRANSFORMED3).build());
    MockSource.writeInput(inputManager, input);
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.FAILED, 5, TimeUnit.MINUTES);
    DataSetManager<Table> tokenizedTextSingle = getDataset(SINGLE_FIELD_DATASET);
    List<StructuredRecord> output = MockSink.readOutput(tokenizedTextSingle);
    Assert.assertEquals(0, output.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInCorrectTypeForFieldToBeTransformed() throws Exception {
    NGramConfig config = new NGramConfig(FIELD_TO_BE_TRANSFORMED, 2, OUTPUT_FIELD);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(SOURCE_SCHEMA_SINGLE_NEGATIVE_TEST);
    new NGramTransform(config).configurePipeline(configurer);
  }

  @Test(expected = NullPointerException.class)
  public void testNullNGramSize() throws Exception {
    buildETLBatchConfig("NegativeTestFornGramSize", SINGLE_FIELD_DATASET, null);
  }

  private Set<List<String>> getExpectedData() {
    Set<List<String>> expected = new HashSet<>();
    expected.add(Arrays.asList("hi i", "i am", "am cdap"));
    expected.add(Arrays.asList("how are", "are you", "you cdap"));
    return expected;
  }

  private Set<List<String>> getExpectedDataFor3N() {
    Set<List<String>> expected = new HashSet<>();
    expected.add(Arrays.asList("hi i am", "i am cdap"));
    expected.add(Arrays.asList("how are you", "are you cdap"));
    return expected;
  }
}
