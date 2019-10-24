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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class NGramConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final String INPUT_FIELD = "inputField";
  private static final String OUTPUT_FIELD = "ngrams";
  private static final Schema VALID_SCHEMA =
    Schema.recordOf("inputSchema",
                    Schema.Field.of(INPUT_FIELD, Schema.arrayOf(Schema.of(Schema.Type.STRING))));

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    NGramConfig config = new NGramConfig(INPUT_FIELD, 2, OUTPUT_FIELD);
    config.validate(failureCollector, VALID_SCHEMA);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInputFieldDoesNotExistConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    NGramConfig config = new NGramConfig("nonExisting", 2, OUTPUT_FIELD);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, NGramConfig.PROPERTY_TRANSFORMED_FIELD);
  }

  @Test
  public void testInputFieldNotArray() {
    Schema inputSchema =
      Schema.recordOf("inputSchema",
                      Schema.Field.of(INPUT_FIELD, Schema.of(Schema.Type.STRING)));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    NGramConfig config = new NGramConfig(INPUT_FIELD, 2, OUTPUT_FIELD);
    config.validate(failureCollector, inputSchema);
    assertValidationFailed(failureCollector, NGramConfig.PROPERTY_TRANSFORMED_FIELD);
  }

  @Test
  public void testInputFieldArrayOfNonStrings() {
    Schema inputSchema =
      Schema.recordOf("inputSchema",
                      Schema.Field.of(INPUT_FIELD, Schema.arrayOf(Schema.of(Schema.Type.INT))));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    NGramConfig config = new NGramConfig(INPUT_FIELD, 2, OUTPUT_FIELD);
    config.validate(failureCollector, inputSchema);
    assertValidationFailed(failureCollector, NGramConfig.PROPERTY_TRANSFORMED_FIELD);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();

    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Nonnull
  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String stacktrace) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(stacktrace) != null)
      .collect(Collectors.toList());
  }
}
