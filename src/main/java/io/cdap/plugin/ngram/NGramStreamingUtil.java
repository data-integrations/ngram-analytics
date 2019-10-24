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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.NGram;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * Util methods for {@link NGramTransform}.
 *
 * This class contains methods for {@link NGramTransform} that require spark classes because during validation
 * spark classes are not available.
 */
public class NGramStreamingUtil {
  public static JavaRDD<StructuredRecord> getRDD(NGramConfig config, SparkExecutionPluginContext context,
                                                 JavaRDD<StructuredRecord> input) {
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SQLContext sqlContext = new SQLContext(javaSparkContext);
    //Create outputschema
    Schema outputSchema = config.getOutputSchema();
    //Schema to be used to create dataframe
    StructType schema = new StructType(new StructField[]{
      new StructField(config.getFieldToBeTransformed(), DataTypes.createArrayType(DataTypes.StringType),
                      false, Metadata.empty())});
    //Transform input i.e JavaRDD<StructuredRecord> to JavaRDD<Row>
    JavaRDD<Row> rowRDD = input.map(new Function<StructuredRecord, Row>() {
      @Override
      public Row call(StructuredRecord rec) {
        // In Spark 2, List is no longer a valid type for schema array<type>
        return RowFactory.create(listToArray(rec.get(config.getFieldToBeTransformed())));
      }
    });
    Dataset wordDataFrame = sqlContext.createDataFrame(rowRDD, schema);
    NGram ngramTransformer = new NGram().setN(config.getNgramSize())
      .setInputCol(config.getFieldToBeTransformed()).setOutputCol(config.getOutputField());
    Dataset ngramDataFrame = ngramTransformer.transform(wordDataFrame);
    JavaRDD<Row> nGramRDD = javaSparkContext.parallelize(ngramDataFrame.select(config.getOutputField())
                                                           .collectAsList());
    //Transform JavaRDD<Row> to JavaRDD<StructuredRecord>
    return nGramRDD.map(new Function<Row, StructuredRecord>() {
      @Override
      public StructuredRecord call(Row row) throws Exception {
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        for (Schema.Field field : outputSchema.getFields()) {
          if (row.getList(0).size() > 0) {
            builder.set(field.getName(), row.getList(0));
          }
        }
        return builder.build();
      }
    });
  }

  private static Object listToArray(Object object) {
    return object instanceof List ? ((List) object).toArray() : object;
  }
}
