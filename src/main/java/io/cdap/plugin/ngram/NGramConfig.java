/*
 * Copyright © 2019 Cask Data, Inc.
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
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import javax.annotation.Nullable;

/**
 * Configuration for the NGramTransform Plugin.
 */
public class NGramConfig extends PluginConfig {
  static final String PROPERTY_TRANSFORMED_FIELD = "fieldToBeTransformed";

  @Name(PROPERTY_TRANSFORMED_FIELD)
  @Description("Field to be used to transform input features into n-grams.")
  private final String fieldToBeTransformed;

  @Description("N-Gram size.")
  @Macro
  private final Integer ngramSize;

  @Description("Transformed field for sequence of n-gram.")
  private final String outputField;


  public NGramConfig(String fieldToBeTransformed, Integer ngramSize, String outputField) {
    this.fieldToBeTransformed = fieldToBeTransformed;
    this.ngramSize = ngramSize;
    this.outputField = outputField;
  }

  public String getFieldToBeTransformed() {
    return fieldToBeTransformed;
  }

  public Integer getNgramSize() {
    return ngramSize;
  }

  public String getOutputField() {
    return outputField;
  }

  public void validate(FailureCollector failureCollector, @Nullable Schema inputSchema) {
    if (inputSchema == null) {
      failureCollector.addFailure("Input schema must be specified.", null);
      return;
    }

    Schema.Field transformedField = inputSchema.getField(fieldToBeTransformed);
    if (transformedField == null) {
      failureCollector.addFailure(String.format("Field '%s' is not present in input schema.", fieldToBeTransformed),
                                  null).withConfigProperty(PROPERTY_TRANSFORMED_FIELD)
        .withInputSchemaField(PROPERTY_TRANSFORMED_FIELD);
    } else {
      Schema transformedFieldSchema = transformedField.getSchema();

      if (transformedFieldSchema.isNullable()) {
        transformedFieldSchema = transformedFieldSchema.getNonNullable();
      }

      Schema componentSchema = null;
      if (transformedFieldSchema.getType() == Schema.Type.ARRAY) {
        componentSchema = transformedFieldSchema.getComponentSchema();

        if (componentSchema.isNullable()) {
          componentSchema = componentSchema.getNonNullable();
        }
      }

      if (transformedFieldSchema.getLogicalType() != null || transformedFieldSchema.getType() != Schema.Type.ARRAY
      || componentSchema.getType() != Schema.Type.STRING) {
        failureCollector.addFailure(String.format("Field '%s' is of unexpected type '%s'.",
                                                  transformedField.getName(), transformedFieldSchema.getDisplayName()),
                                    "Supported type is 'array of strings'.")
          .withConfigProperty(PROPERTY_TRANSFORMED_FIELD)
          .withInputSchemaField(PROPERTY_TRANSFORMED_FIELD);
      }
    }
  }

  public Schema getOutputSchema() {
    return Schema.recordOf("outputSchema", Schema.Field.of(outputField,
                                                           Schema.arrayOf(Schema.of(Schema.Type.STRING))));
  }
}
