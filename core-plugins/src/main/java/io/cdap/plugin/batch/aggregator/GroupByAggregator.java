/*
 * Copyright © 2016-2020 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
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
import io.cdap.cdap.etl.api.aggregation.GroupByAggregationDefinition;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.engine.sql.StandardSQLCapabilities;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.cdap.etl.api.relational.CoreExpressionCapabilities;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.LinearRelationalTransform;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;
import io.cdap.plugin.batch.aggregator.function.AggregateFunction;
import io.cdap.plugin.batch.aggregator.function.JexlCondition;
import io.cdap.plugin.common.SchemaValidator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Batch group by aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("GroupByAggregate")
@Description("Groups by one or more fields, then performs one or more aggregate functions on each group. " +
  "Supports `Average`, `Count`, `First`, `Last`, `Max`, `Min`,`Sum`,`Collect List`,`Collect Set`, " +
  "`Standard Deviation`, `Variance`, `Count Distinct` as aggregate functions.")
public class GroupByAggregator extends RecordReducibleAggregator<AggregateResult>
  implements LinearRelationalTransform {
  private final GroupByConfig conf;
  private final HashMap<String, String> functionNameMap = new HashMap<String, String>() {{
    put("AVG", "Avg");
    put("COUNT", "Count");
    put("FIRST", "First");
    put("LAST", "Last");
    put("MAX", "Max");
    put("MIN", "Min");
    put("STDDEV", "Stddev");
    put("SUM", "Sum");
    put("VARIANCE", "Variance");
    put("COLLECTLIST", "CollectList");
    put("COLLECTSET", "CollectSet");
    put("COUNTDISTINCT", "CountDistinct");
    put("LONGESTSTRING", "LongestString");
    put("SHORTESTSTRING", "ShortestString");
    put("COUNTNULLS", "CountNulls");
    put("CONCAT", "Concat");
    put("CONCATDISTINCT", "ConcatDistinct");
    put("LOGICALAND", "LogicalAnd");
    put("LOGICALOR", "LogicalOr");
    put("CORRECTEDSUMOFSQUARES", "CorrectedSumOfSquares");
    put("SUMOFSQUARES", "SumOfSquares");
    put("COUNTIF", "CountIf");
    put("COUNTDISTINCTIF", "CountDistinctIf");
    put("SUMIF", "SumIf");
    put("AVGIF", "AvgIf");
    put("MINIF", "MinIf");
    put("MAXIF", "MaxIf");
    put("STDDEVIF", "StddevIf");
    put("VARIANCEIF", "VarianceIf");
    put("COLLECTLISTIF", "CollectListIf");
    put("COLLECTSETIF", "CollectSetIf");
    put("LONGESTSTRINGIF", "LongSetStringIf");
    put("SHORTESTSTRINGIF", "ShortestStringIf");
    put("CONCATIF", "ConcatIf");
    put("CONCATDISTINCTIF", "ConcatDistinctIf");
    put("LOGICALANDIF", "LogicalAndIf");
    put("LOGICALORIF", "LogicalOrIf");
    put("CORRECTEDSUMOFSQUARESIF", "CorrectedSumOfSquaresIf");
    put("SUMOFSQUARESIF", "SumOfSquaresIf");
    put("ANYIF", "AnyIf");
  }};

  // Ansi SQL aggregations
  private final HashMap<GroupByConfig.Function, String> functionSqlMap =
    new HashMap<GroupByConfig.Function, String>() {{
      put(GroupByConfig.Function.AVG, "AVG(%s)");
      put(GroupByConfig.Function.MAX, "MAX(%s)");
      put(GroupByConfig.Function.MIN, "MIN(%s)");
      put(GroupByConfig.Function.STDDEV, "STDDEV_POP(%s)");
      put(GroupByConfig.Function.SUM, "SUM(%s)");
      put(GroupByConfig.Function.VARIANCE, "VAR_POP(%s)");
      put(GroupByConfig.Function.COUNT, "COUNT(%s)");
      put(GroupByConfig.Function.COUNTNULLS, "SUM(CASE WHEN %s IS NULL THEN 1 ELSE 0 END)");
      put(GroupByConfig.Function.COUNTDISTINCT,
          "COUNT(DISTINCT %s) + COALESCE(MAX(CASE WHEN %<s IS NULL THEN 1 ELSE 0 END), 0)");
      put(GroupByConfig.Function.SUMOFSQUARES, "CASE WHEN COUNT(%s) > 0 THEN SUM(POWER(%<s, 2)) ELSE 0 END");
      put(GroupByConfig.Function.CORRECTEDSUMOFSQUARES,
          "CASE WHEN COUNT(%s) > 1 THEN SUM(POWER(%<s, 2)) - (POWER(SUM(%<s), 2)/COUNT(%<s)) ELSE 0 END");
    }};

  // BigQuery specific aggregations
  private final HashMap<GroupByConfig.Function, String> functionBQSqlMap =
    new HashMap<GroupByConfig.Function, String>() {{
      put(GroupByConfig.Function.COLLECTLIST, "ARRAY_AGG(%s IGNORE NULLS)");
      put(GroupByConfig.Function.COLLECTSET, "ARRAY_AGG(DISTINCT %s IGNORE NULLS)");
      put(GroupByConfig.Function.CONCAT, "STRING_AGG(CAST(%s AS STRING), \", \")");
      put(GroupByConfig.Function.CONCATDISTINCT, "STRING_AGG(DISTINCT CAST(%s AS STRING) , \", \")");
      put(GroupByConfig.Function.LOGICALAND, "COALESCE(LOGICAL_AND(%s), TRUE)");
      put(GroupByConfig.Function.LOGICALOR, "COALESCE(LOGICAL_OR(%s), FALSE)");
      put(GroupByConfig.Function.SHORTESTSTRING,
          "STRING_AGG(CAST(%s AS STRING) ORDER BY LENGTH(CAST(%<s AS STRING)) ASC LIMIT 1)");
      put(GroupByConfig.Function.LONGESTSTRING,
          "STRING_AGG(CAST(%s AS STRING) ORDER BY LENGTH(CAST(%<s AS STRING)) DESC LIMIT 1)");
    }};

  private List<String> groupByFields;
  private List<GroupByConfig.FunctionInfo> functionInfos;
  private Schema outputSchema;
  private GroupByAggregationDefinition aggregationDefinition;

  public GroupByAggregator(GroupByConfig conf) {
    super(conf.numPartitions);
    this.conf = conf;
  }

  @VisibleForTesting
  GroupByAggregationDefinition getAggregationDefinition() {
    return aggregationDefinition;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    List<String> groupByFields = conf.getGroupByFields();
    List<GroupByConfig.FunctionInfo> aggregates = conf.getAggregates();

    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    if (!conf.containsMacro(AggregatorConfig.NUM_PARTITIONS) && conf.numPartitions != null
        && conf.numPartitions < 0) {
      stageConfigurer.getFailureCollector()
          .addFailure("Number of Partitions cannot be less than zero.", null)
          .withConfigProperty(AggregatorConfig.NUM_PARTITIONS);
    }
    // if null, the input schema is unknown, or its multiple schemas.
    // if groupByFields is empty or aggregates is empty, that means they contain macros, which means the
    // output schema is not known at configure time.
    if (inputSchema == null || groupByFields.isEmpty() || aggregates.isEmpty()) {
      stageConfigurer.setOutputSchema(null);
      return;
    }

    validate(inputSchema, groupByFields, aggregates, stageConfigurer.getFailureCollector());
    //Throw here to avoid throwing IllegalArgumentExceptions in the next function call
    stageConfigurer.getFailureCollector().getOrThrowException();

    // otherwise, we have a constant input schema. Get the output schema and
    // propagate the schema, which is group by fields + aggregate fields
    stageConfigurer.setOutputSchema(getOutputSchema(inputSchema, groupByFields, aggregates));
  }

  public void validate(Schema inputSchema, List<String> groupByFields,
                       List<GroupByConfig.FunctionInfo> aggregates, FailureCollector collector) {

    for (String groupByField : groupByFields) {
      Schema.Field field = inputSchema.getField(groupByField);
      if (field == null) {
        collector.addFailure(String.format("Cannot group by field '%s' because it does not exist in input schema.",
                                           groupByField), null)
          .withConfigElement("groupByFields", groupByField);
      }
    }

    for (GroupByConfig.FunctionInfo functionInfo : aggregates) {
      if (functionInfo.getField().equals("*")) {
        continue;
      }
      Schema.Field inputField = inputSchema.getField(functionInfo.getField());
      String collectorFieldName = String.format("%s:%s(%s)", functionInfo.getName(),
                                              functionNameMap.get(functionInfo.getFunction().toString().toUpperCase()),
                                              functionInfo.getField());

      if (inputField == null) {
        collector.addFailure(
          String.format("Invalid aggregate %s(%s): Field '%s' does not exist in input schema.",
                        functionInfo.getFunction(), functionInfo.getField(), functionInfo.getField()), null)
          .withConfigElement("aggregates", collectorFieldName);
      }

      // TODO: CDAP-16401 - Push down validation to individual aggregate functions
      if (GroupByConfig.Function.COUNTDISTINCT == functionInfo.getFunction()) {
        validateCountDistinct(inputField, collector, collectorFieldName);
      }
    }
    validateConditionalFunctions(inputSchema, conf.getAggregates(), collector);
  }

  private void validateCountDistinct(Schema.Field inputField, FailureCollector collector, String validationFieldName) {
    if (inputField != null) {
      Schema.Type type = inputField.getSchema().isNullable() ?
        inputField.getSchema().getNonNullable().getType() :
        inputField.getSchema().getType();
      if (type != Schema.Type.STRING && type != Schema.Type.INT && type != Schema.Type.LONG
        && type != Schema.Type.BOOLEAN) {
        collector.addFailure(
          String.format("Distinct counting is not supported for the field %s of type %s.", inputField.getName(), type),
          "Please specify a string, integer, long or boolean field.")
          .withConfigElement("aggregates", validationFieldName);
      }
    }
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    super.prepareRun(context);
    validate(context.getInputSchema(), conf.getGroupByFields(), conf.getAggregates(), context.getFailureCollector());
    context.getFailureCollector().getOrThrowException();
    LinkedList<FieldOperation> fllOperations = new LinkedList<>();
    // in configurePipeline all the necessary checks have been performed already to set output schema
    if (SchemaValidator.canRecordLineage(context.getOutputSchema(), "output")) {
      Schema inputSchema = context.getInputSchema();
      // for every function record the field level operation details
      for (GroupByConfig.FunctionInfo functionInfo : conf.getAggregates()) {
        Schema.Field outputSchemaField = getOutputSchemaField(functionInfo, inputSchema);
        String operationName = String.format("Group %s", functionInfo.getField());
        String description = String.format("Aggregate function applied: '%s'.", functionInfo.getFunction());
        FieldOperation operation = new FieldTransformOperation(operationName, description,
                                                               Collections.singletonList(functionInfo.getField()),
                                                               outputSchemaField.getName());
        fllOperations.add(operation);
      }
    }
    context.record(fllOperations);
  }

  private void validateConditionalFunctions(Schema outputSchema, List<GroupByConfig.FunctionInfo> aggregates,
                                            FailureCollector failureCollector) {
    // skip if output schema is null
    if (outputSchema == null) {
      return;
    }
    for (GroupByConfig.FunctionInfo aggregate : aggregates) {
      if (aggregate.getFunction().isConditional()) {
        Set<List<String>> variables = JexlCondition.getVariables(aggregate.getCondition());
        for (List<String> variable : variables) {
          if (!fieldExistsInOutputSchema(variable, outputSchema)) {
            failureCollector.addFailure(String.format("Field %s not found in output schema.", variable),
                                        "Fields used in conditions are required to be available in output schema.")
              .withConfigElement("aggregates", String.join(".", variable));
          }
        }
      }
    }
  }

  /**
   * Checks if field exists in a schema.
   *
   * @param path   list of string representing path of the field.
   * @param schema {@link Schema} schema that could contain the field this method is checking for.
   * @return {@link Boolean} indicator whether field exists on schema or not.
   *
   * Example for nested record:
   * <pre>{@code
   * {
   *   "author": {
   *     "name": "Jan Doe",
   *     "contact": {
   *        "e-mail": "jan@doe.com"
   *                }
   *            }
   * }
   * }</pre>
   *
   * and the condition is like this:
   * author.contact.e-mail.equals('jan@doe.com')
   */
  private boolean fieldExistsInOutputSchema(List<String> path, Schema schema) {
    if (path.size() == 0) {
      return false;
    }
    Schema.Field currentField = schema.getField(path.get(0));
    if (currentField == null) {
      return false;
    }
    if (path.size() > 1) {
      return fieldExistsInOutputSchema(path.subList(1, path.size() - 1), currentField.getSchema());
    }
    return true;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    groupByFields = conf.getGroupByFields();
    functionInfos = conf.getAggregates();
    if (context.getInputSchema() != null) {
      initAggregates(context.getInputSchema());
    }
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) {
    // app should provide some way to make some data calculated in configurePipeline available here.
    // then we wouldn't have to calculate schema here
    StructuredRecord.Builder builder = StructuredRecord.builder(getGroupKeySchema(record.getSchema()));
    for (String groupByField : conf.getGroupByFields()) {
      builder.set(groupByField, record.get(groupByField));
    }
    emitter.emit(builder.build());
  }

  @Override
  public AggregateResult initializeAggregateValue(StructuredRecord record) {
    Map<String, AggregateFunction> functions = initAggregates(record.getSchema());
    functions.values().forEach(AggregateFunction::initialize);
    updateAggregates(functions, record);
    return new AggregateResult(record.getSchema(), functions);
  }

  @Override
  public AggregateResult mergeValues(AggregateResult agg, StructuredRecord record) {
    updateAggregates(agg.getFunctions(), record);
    return agg;
  }

  @Override
  public AggregateResult mergePartitions(AggregateResult agg1, AggregateResult agg2) {
    mergeAggregates(agg1.getFunctions(), agg2.getFunctions());
    return agg1;
  }

  @Override
  public void finalize(StructuredRecord groupKey, AggregateResult aggValue,
                       Emitter<StructuredRecord> emitter) {
    initAggregates(aggValue.getInputSchema());
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (String groupByField : groupByFields) {
      builder.set(groupByField, groupKey.get(groupByField));
    }

    for (Map.Entry<String, AggregateFunction> aggregateFunction : aggValue.getFunctions().entrySet()) {
      builder.set(aggregateFunction.getKey(), aggregateFunction.getValue().getAggregate());
    }
    emitter.emit(builder.build());
  }

  private Schema getOutputSchema(Schema inputSchema, List<String> groupByFields,
                                 List<GroupByConfig.FunctionInfo> aggregates) {
    // Check that all the group by fields exist in the input schema,
    List<Schema.Field> outputFields = new ArrayList<>(groupByFields.size() + aggregates.size());
    for (String groupByField : groupByFields) {
      Schema.Field field = inputSchema.getField(groupByField);
      if (field == null) {
        String error = String.format(
          "Cannot group by field '%s' because it does not exist in input schema %s.",
          groupByField, inputSchema);
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
          error, error, ErrorType.USER, false, null);
      }
      outputFields.add(field);
    }

    // add all the required output field schema depending on the aggregate functions
    for (GroupByConfig.FunctionInfo functionInfo : aggregates) {
      outputFields.add(getOutputSchemaField(functionInfo, inputSchema));
    }
    return Schema.recordOf(inputSchema.getRecordName() + ".agg", outputFields);
  }

  private void updateAggregates(Map<String, AggregateFunction> aggregateFunctions, StructuredRecord groupVal) {
    for (AggregateFunction aggregateFunction : aggregateFunctions.values()) {
      aggregateFunction.mergeValue(groupVal);
    }
  }

  private void mergeAggregates(Map<String, AggregateFunction> agg1, Map<String, AggregateFunction> agg2) {
    for (Map.Entry<String, AggregateFunction> aggregateFunction : agg1.entrySet()) {
      aggregateFunction.getValue().mergeAggregates(agg2.get(aggregateFunction.getKey()));
    }
  }

  private Schema.Field getOutputSchemaField(GroupByConfig.FunctionInfo functionInfo, Schema inputSchema) {
    // special case count(*) because we don't have to check that the input field exists
    if (functionInfo.getField().equals("*")) {
      AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(null);
      return Schema.Field.of(functionInfo.getName(), aggregateFunction.getOutputSchema());
    }

    Schema.Field inputField = inputSchema.getField(functionInfo.getField());
    if (inputField == null) {
      String error = String.format(
        "Invalid aggregate %s(%s): Field '%s' does not exist in input schema %s.",
        functionInfo.getFunction(), functionInfo.getField(), functionInfo.getField(), inputSchema);
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        error, error, ErrorType.USER, false, null);
    }
    AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(inputField.getSchema());
    return Schema.Field.of(functionInfo.getName(), aggregateFunction.getOutputSchema());
  }

  private Map<String, AggregateFunction> initAggregates(Schema valueSchema) {
    List<Schema.Field> outputFields = new ArrayList<>(groupByFields.size() + functionInfos.size());
    for (String groupByField : groupByFields) {
      outputFields.add(valueSchema.getField(groupByField));
    }

    Map<String, AggregateFunction> functions = new HashMap<>();
    for (GroupByConfig.FunctionInfo functionInfo : functionInfos) {
      Schema.Field inputField = valueSchema.getField(functionInfo.getField());
      Schema fieldSchema = inputField == null ? null : inputField.getSchema();
      AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(fieldSchema);
      outputFields.add(Schema.Field.of(functionInfo.getName(), aggregateFunction.getOutputSchema()));
      functions.put(functionInfo.getName(), aggregateFunction);
    }
    outputSchema = Schema.recordOf(valueSchema.getRecordName() + ".agg", outputFields);
    return functions;
  }

  private Schema getGroupKeySchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    for (String groupByField : conf.getGroupByFields()) {
      Schema.Field fieldSchema = inputSchema.getField(groupByField);
      if (fieldSchema == null) {
        String error = String.format(
          "Cannot group by field '%s' because it does not exist in input schema %s",
          groupByField, inputSchema);
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
          error, error, ErrorType.USER, false, null);
      }
      fields.add(fieldSchema);
    }
    return Schema.recordOf("group.key.schema", fields);
  }

  @Override
  public Relation transform(RelationalTranformContext relationalTranformContext, Relation relation) {
    // Check if this aggregation definition is supported in SQL
    if (!areAllAggregatesSupportedInRelationalTransform()) {
      return new InvalidRelation("Unsupported aggregation definition");
    }

    // Get an expression factory for this transform context
    Optional<ExpressionFactory<String>> expressionFactory = getExpressionFactory(relationalTranformContext);
    if (!expressionFactory.isPresent()) {
      return new InvalidRelation("Cannot find an Expression Factory");
    }

    GroupByAggregationDefinition aggregationDefinition = getAggregationDefinition(expressionFactory.get(), relation);

    // If the aggregation definition is null, we cannot support this aggregation.
    if (aggregationDefinition == null) {
      return new InvalidRelation("Unsupported aggregation definition");
    }
    return relation.groupBy(aggregationDefinition);
  }

  /**
   * Function used to determine if all aggregations have a SQL equivalent definition
   * @return true if all aggregations have SQL representation, false if not.
   */
  private boolean areAllAggregatesSupportedInRelationalTransform() {
    for (GroupByConfig.FunctionInfo aggregate : conf.getAggregates()) {
      GroupByConfig.Function func = aggregate.getFunction();
      // If the function is not supported in ANSI SQL or BigQuery, this relation is not supported by this engine.
      if (!functionSqlMap.containsKey(func) && !functionBQSqlMap.containsKey(func)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns an expression factory for the supplied relational transform context.
   * This function uses the aggregation definition to dedice which capabilities are needed from the engine.
   *
   * @param ctx transform context
   * @return {@link Optional} containing the Expression factory to use, or null if this definition is not supported
   * by the engine
   */
  private Optional<ExpressionFactory<String>> getExpressionFactory(RelationalTranformContext ctx) {
    List<GroupByConfig.FunctionInfo> functionInfos = conf.getAggregates();
    boolean requiresBigQueryCapability = false;

    for (GroupByConfig.FunctionInfo aggregate : functionInfos) {
      GroupByConfig.Function func = aggregate.getFunction();
      // If the function is not supported in ANSI SQL or BigQuery, this relation is not supported by this engine.
      if (!functionSqlMap.containsKey(func) && !functionBQSqlMap.containsKey(func)) {
        return Optional.empty();
      }

      // If any of the functions requires BigQuery, set the requiresBigQueryCapability flag to true;
      if (functionBQSqlMap.containsKey(func)) {
        requiresBigQueryCapability = true;
      }
    }

    // If the BigQuery capability is required, ensure the SQL engine supports this capability.
    return requiresBigQueryCapability ?
      ctx.getEngine().getExpressionFactory(StringExpressionFactoryType.SQL, StandardSQLCapabilities.BIGQUERY) :
      ctx.getEngine().getExpressionFactory(StringExpressionFactoryType.SQL);
  }

  private GroupByAggregationDefinition getAggregationDefinition(ExpressionFactory<String> expressionFactory,
                                                                Relation relation) {
    List<String> groupByFields = conf.getGroupByFields();
    List<GroupByConfig.FunctionInfo> functionInfos = conf.getAggregates();

    List<Expression> groupByExpressions = new ArrayList<>(groupByFields.size());
    Map<String, Expression> selectExpressions = new HashMap<>();

    for (String field : groupByFields) {
      String columnName = getColumnName(expressionFactory, relation, field);
      Expression groupByExpression = expressionFactory.compile(columnName);
      groupByExpressions.add(groupByExpression);
      selectExpressions.put(field, groupByExpression);
    }

    for (GroupByConfig.FunctionInfo aggregate : functionInfos) {
      String alias = aggregate.getName();
      String columnName = getColumnName(expressionFactory, relation, aggregate.getField());
      GroupByConfig.Function function = aggregate.getFunction();

      // Check if this function is supported in ANSI SQL
      if (functionSqlMap.containsKey(function)) {
        String selectSql = String.format(functionSqlMap.get(function), columnName);
        selectExpressions.put(alias, expressionFactory.compile(selectSql));
        continue;
      }

      // Check if this function is supported in BigQuery.
      if (functionBQSqlMap.containsKey(function)
        && expressionFactory.getCapabilities().contains(StandardSQLCapabilities.BIGQUERY)) {
        String selectSql = String.format(functionBQSqlMap.get(function), columnName);
        selectExpressions.put(alias, expressionFactory.compile(selectSql));
        continue;
      }

      return null;
    }

    aggregationDefinition = GroupByAggregationDefinition.builder()
      .select(selectExpressions)
      .groupBy(groupByExpressions)
      .build();
    return aggregationDefinition;
  }

  private String getColumnName(ExpressionFactory<String> expressionFactory, Relation relation, String name) {
    // If the column name is *, return as such.
    if ("*".equals(name)) {
      return name;
    }

    // Verify if the expression factory can provide a quoted column name, and use this if available.
    if (expressionFactory.getCapabilities().contains(CoreExpressionCapabilities.CAN_GET_QUALIFIED_COLUMN_NAME)) {
      return expressionFactory.getQualifiedColumnName(relation, name).extract();
    }

    // Return supplied column name.
    return name;
  }
}
