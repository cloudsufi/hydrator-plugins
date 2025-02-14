/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.aggregator;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.LinearRelationalTransform;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;
import io.cdap.plugin.batch.aggregator.function.SelectionFunction;
import io.cdap.plugin.common.SchemaValidator;
import io.cdap.plugin.common.TransformLineageRecorderUtils;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;


/**
 * Deduplicate aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("Deduplicate")
@Description("Deduplicates input records, optionally restricted to one or more fields. Takes an optional " +
  "filter function to choose one or more records based on a specific field and a selection function.")
public class DedupAggregator extends RecordReducibleAggregator<StructuredRecord> implements LinearRelationalTransform {
  private final DedupConfig dedupConfig;
  private List<String> uniqueFields;
  private DedupConfig.DedupFunctionInfo filterFunction;
  private SelectionFunction selectionFunction;
  private static final EnumSet<Schema.Type> ALLOWED_SCHEMA_TYPES = EnumSet.of(Schema.Type.INT, Schema.Type.LONG,
          Schema.Type.FLOAT, Schema.Type.DOUBLE);
  private static final EnumSet<Schema.LogicalType> ALLOWED_LOGICAL_SCHEMA_TYPES = EnumSet.of(Schema.LogicalType.DATE,
          Schema.LogicalType.DATETIME, Schema.LogicalType.DECIMAL, Schema.LogicalType.TIME_MICROS,
          Schema.LogicalType.TIME_MILLIS, Schema.LogicalType.TIMESTAMP_MILLIS, Schema.LogicalType.TIMESTAMP_MICROS);

  public DedupAggregator(DedupConfig dedupConfig) {
    super(dedupConfig.numPartitions);
    this.dedupConfig = dedupConfig;
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    super.prepareRun(context);

    // in configurePipeline all the necessary checks have been performed already to set output schema
    if (SchemaValidator.canRecordLineage(context.getOutputSchema(), context.getStageName())) {
      TransformLineageRecorderUtils.generateOneToOnes(
              TransformLineageRecorderUtils.getFields(context.getInputSchema()),
              "dedup",
              "Removed duplicate records based on unique fields.");
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    List<String> uniqueFields = dedupConfig.getUniqueFields();
    DedupConfig.DedupFunctionInfo functionInfo = dedupConfig.getFilter();

    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    // if null, the input schema is unknown, or its multiple schemas.
    if (inputSchema == null) {
      stageConfigurer.setOutputSchema(null);
      return;
    }

    // otherwise, we have a constant input schema. Get the output schema and propagate the schema
    Schema outputSchema = getOutputSchema(inputSchema);
    FailureCollector collector = stageConfigurer.getFailureCollector();
    validateSchema(outputSchema, uniqueFields, functionInfo, collector);

    if (functionInfo != null) {
      // Invoke to validate whether the function used is supported, the field must be non-null here because of the
      // validation before
      functionInfo.getSelectionFunction(outputSchema.getField(functionInfo.getField()).getSchema());
    }

    stageConfigurer.setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) {
    uniqueFields = dedupConfig.getUniqueFields();
    filterFunction = dedupConfig.getFilter();
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) {
    if (uniqueFields == null) {
      emitter.emit(record);
      return;
    }

    StructuredRecord.Builder builder = StructuredRecord.builder(getGroupKeySchema(record.getSchema()));
    for (String fieldName : uniqueFields) {
      builder.set(fieldName, record.get(fieldName));
    }
    emitter.emit(builder.build());
  }

  @Override
  public StructuredRecord initializeAggregateValue(StructuredRecord record) {
    return record;
  }

  @Override
  public StructuredRecord mergeValues(StructuredRecord aggValue, StructuredRecord record) {
    return select(aggValue, record);
  }

  @Override
  public StructuredRecord mergePartitions(StructuredRecord aggValue1, StructuredRecord aggValue2) {
    return select(aggValue1, aggValue2);
  }

  @Override
  public void finalize(StructuredRecord groupKey, StructuredRecord aggVal, Emitter<StructuredRecord> emitter) {
    emitter.emit(aggVal);
  }

  private StructuredRecord select(StructuredRecord record1, StructuredRecord record2) {
    if (filterFunction == null) {
      return record1;
    }

    // TODO: CDAP-16473 after we propagate schema in prepareRun, this validation can happen in prepareRun, and
    // initialize method can create this variable
    if (selectionFunction == null) {
      Schema.Field field = record1.getSchema().getField(filterFunction.getField());
      if (field == null) {
        String error = String.format("Failed to merge values because the field '%s' cannot be used as a " +
          "filter field since it does not exist in the output schema", filterFunction.getField());
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
          error, error, ErrorType.USER, false, null);
      }
      selectionFunction = filterFunction.getSelectionFunction(field.getSchema());
    }
    return selectionFunction.select(record1, record2);
  }

  private Schema getGroupKeySchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    for (String fieldName : dedupConfig.getUniqueFields()) {
      Schema.Field field = inputSchema.getField(fieldName);
      if (field == null) {
        String error = String.format("Failed to groupBy because field %s does not exist in input schema %s.",
          fieldName, inputSchema);
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
          error, error, ErrorType.USER, false, null);
      }
      fields.add(field);
    }
    return Schema.recordOf(inputSchema.getRecordName() + ".unique", fields);
  }

  private Schema getOutputSchema(Schema inputSchema) {
    return Schema.recordOf(inputSchema.getRecordName() + ".dedup", inputSchema.getFields());
  }

  private void validateSchema(Schema inputSchema, List<String> uniqueFields,
                              @Nullable DedupConfig.DedupFunctionInfo function, FailureCollector collector) {
    for (String uniqueField : uniqueFields) {
      Schema.Field field = inputSchema.getField(uniqueField);
      if (field == null) {
        collector.addFailure(String.format("Field '%s' does not exist in the input schema", uniqueField), "")
                .withConfigElement("uniqueFields", uniqueField);
      }
    }

    if (function != null) {
      Schema.Field field = inputSchema.getField(function.getField());
      if (field == null) {
        collector.addFailure(String.format("Invalid filter %s(%s): Field '%s' does not exist in input schema ",
                                function.getFunction(), function.getField(), function.getField()),
                        null)
                .withConfigProperty("filterOperation");
      }
      Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
      Schema.Type fieldType = fieldSchema.getType();
      Schema.LogicalType logicalFieldType = fieldSchema.getLogicalType();
      if ((function.getFunction() == DedupConfig.Function.MAX || function.getFunction() == DedupConfig.Function.MIN)
        && (!ALLOWED_SCHEMA_TYPES.contains(fieldType) && !ALLOWED_LOGICAL_SCHEMA_TYPES.contains(logicalFieldType))) {
        collector.addFailure(String.format("Unsupported filter operation %s(%s): Field has a type that is not " +
                                        "supported for deduplication operations",
                                function.getFunction(), function.getField(), function.getField()),
                        null)
                .withConfigProperty("filterOperation");
      }
  }
  }

  @Override
  public boolean canUseEngine(Engine engine) {
    Optional<ExpressionFactory<String>> expressionFactory = engine.
            getExpressionFactory(StringExpressionFactoryType.SQL);
    return expressionFactory.isPresent();
  }

  @Override
  public Relation transform(RelationalTranformContext relationalTranformContext, Relation relation) {
    DeduplicateAggregationDefinition deduplicateAggregationDefinition = DedupAggregatorUtils
            .generateAggregationDefinition(relationalTranformContext,
                                           relation,
                                           dedupConfig.getUniqueFields(),
                                           dedupConfig.getFilter()
            );

    if (deduplicateAggregationDefinition == null) {
      return new InvalidRelation("Filter Operation is not supported. Only ANY, MIN and MAX are supported.");
    }

    return relation.deduplicate(deduplicateAggregationDefinition);
  }
}
