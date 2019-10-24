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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.api.java.JavaRDD;

/**
 * NGramTransform - SparkCompute to transform input features into n-grams.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(NGramTransform.PLUGIN_NAME)
@Description("Used to transform input features into n-grams.")
public class NGramTransform extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String PLUGIN_NAME = "NGramTransform";
  private NGramConfig config;

  public NGramTransform(NGramConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();

    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector, inputSchema);

    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getOutputSchema());
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) {
    return NGramStreamingUtil.getRDD(config, context, input);
  }
}
