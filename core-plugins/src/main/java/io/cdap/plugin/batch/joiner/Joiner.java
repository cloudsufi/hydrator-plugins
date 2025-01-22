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

package io.cdap.plugin.batch.joiner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerContext;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.InvalidJoinException;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.api.join.error.JoinError;
import io.cdap.cdap.etl.api.join.error.OutputSchemaError;
import io.cdap.cdap.etl.api.join.error.SelectedFieldError;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Batch joiner to join records from multiple inputs
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name("Joiner")
@Description("Performs join operation on records from each input based on required inputs. If all the inputs are " +
  "required inputs, inner join will be performed. Otherwise inner join will be performed on required inputs and " +
  "records from non-required inputs will only be present if they match join criteria. If there are no required " +
  "inputs, outer join will be performed")
public class Joiner extends BatchAutoJoiner {

  private static final Logger LOG = LoggerFactory.getLogger(Joiner.class);
  public static final String JOIN_OPERATION_DESCRIPTION = "Used as a key in a join";
  public static final String IDENTITY_OPERATION_DESCRIPTION = "Unchanged as part of a join";
  public static final String RENAME_OPERATION_DESCRIPTION = "Renamed as a part of a join";

  private final JoinerConfig conf;

  public Joiner(JoinerConfig conf) {
    this.conf = conf;
  }

  @Nullable
  @Override
  public JoinDefinition define(AutoJoinerContext context) {
    FailureCollector collector = context.getFailureCollector();

    boolean hasUnknownInputSchema = context.getInputStages().values().stream().anyMatch(Objects::isNull);
    if (hasUnknownInputSchema && !conf.containsMacro(JoinerConfig.OUTPUT_SCHEMA) &&
      conf.getOutputSchema(collector) == null) {
      // If input schemas are unknown, an output schema must be provided.
      collector.addFailure("Output schema must be specified", null).withConfigProperty(JoinerConfig.OUTPUT_SCHEMA);
    }
    if (!conf.containsMacro(JoinerConfig.NUM_PARTITIONS) && conf.getNumPartitions() != null &&
      conf.getNumPartitions() < 0) {
      collector.addFailure("Number of Partitions cannot be less than zero.", null)
        .withConfigProperty(JoinerConfig.NUM_PARTITIONS);
    }
    if (conf.requiredPropertiesContainMacros()) {
      return null;
    }

    Set<String> requiredStages = conf.getRequiredInputs();
    Set<String> broadcastStages = conf.getBroadcastInputs();
    List<JoinStage> inputs = new ArrayList<>(context.getInputStages().size());
    boolean useOutputSchema = false;
    for (JoinStage joinStage : context.getInputStages().values()) {
      inputs.add(JoinStage.builder(joinStage)
                   .setRequired(requiredStages.contains(joinStage.getStageName()))
                   .setBroadcast(broadcastStages.contains(joinStage.getStageName()))
                   .build());
      useOutputSchema = useOutputSchema || joinStage.getSchema() == null;
    }

    JoinCondition condition = conf.getCondition(collector);
    if (condition.getOp() == JoinCondition.Op.EXPRESSION) {
      if (inputs.size() != 2) {
        collector.addFailure("Advanced join conditions can only be used when there are two inputs.", null)
          .withConfigProperty(JoinerConfig.CONDITION_TYPE);
        throw collector.getOrThrowException();
      }

      /*
         If this is an outer join of some kind and it is not a broadcast join, add a failure.
         this is because any outer join that is not an equality join in Spark will get turned into
         a BroadcastNestedLoopJoin anyway. So it is better to make that behavior explicit to the user
         and force them to specify which side should be broadcast. This also prevents problems where
         Spark will just choose to broadcast the right side because it doesn't know how big the input datasets are.
         See CDAP-17718 for more info.
       */
      if (requiredStages.size() < inputs.size() && broadcastStages.isEmpty()) {
        collector.addFailure("Advanced outer joins must specify an input to load in memory.", null)
          .withConfigProperty(JoinerConfig.MEMORY_INPUTS);
      }
    }

    // Validate Join Left Side property
    if (!conf.distributionStageNameConstainsMacro()
      && !Strings.isNullOrEmpty(conf.getDistributionStageName())
      && inputs.stream()
      .map(JoinStage::getStageName)
      .noneMatch(sn -> Objects.equals(sn, conf.getDistributionStageName()))) {
      collector.addFailure("Only one stage can be specified as the stage with the larger skew.",
                           "Please select only one stage.")
        .withConfigProperty(JoinerConfig.DISTRIBUTION_STAGE);
    }


    try {
      JoinDefinition.Builder joinBuilder = JoinDefinition.builder();

      // If the user has specified one side as the most skewed, move this to the left side of the join.
      // This is useful for BigQuery Pushdown Joins
      if (!conf.distributionStageNameConstainsMacro() && !Strings.isNullOrEmpty(conf.getDistributionStageName())) {
        reorderJoinStages(inputs, conf.getDistributionStageName());
      }

      joinBuilder
        .select(conf.getSelectedFields(collector))
        .from(inputs)
        .on(condition);
      if (useOutputSchema) {
        joinBuilder.setOutputSchema(conf.getOutputSchema(collector));
      } else {
        joinBuilder.setOutputSchemaName("join.output");
      }

      if (conf.isDistributionValid(collector)) {
        joinBuilder.setDistributionFactor(conf.getDistributionFactor(), conf.getDistributionStageName());
      }
      return joinBuilder.build();
    } catch (InvalidJoinException e) {
      if (e.getErrors().isEmpty()) {
        collector.addFailure(e.getMessage(), null);
      }
      for (JoinError error : e.getErrors()) {
        ValidationFailure failure = collector.addFailure(error.getMessage(), error.getCorrectiveAction());
        switch (error.getType()) {
          case JOIN_KEY:
          case JOIN_KEY_FIELD:
            failure.withConfigProperty(JoinerConfig.JOIN_KEYS);
            break;
          case SELECTED_FIELD:
            JoinField badField = ((SelectedFieldError) error).getField();
            failure.withConfigElement(
              JoinerConfig.SELECTED_FIELDS,
              String.format("%s.%s as %s", badField.getStageName(), badField.getFieldName(), badField.getAlias()));
            break;
          case OUTPUT_SCHEMA:
            OutputSchemaError schemaError = (OutputSchemaError) error;
            failure.withOutputSchemaField(schemaError.getField());
            break;
          case DISTRIBUTION_SIZE:
            failure.withConfigProperty(JoinerConfig.DISTRIBUTION_FACTOR);
            break;
          case DISTRIBUTION_STAGE:
            failure.withConfigProperty(JoinerConfig.DISTRIBUTION_STAGE);
            break;
          case BROADCAST:
            failure.withConfigProperty(JoinerConfig.MEMORY_INPUTS);
            break;
          case INVALID_CONDITION:
            failure.withConfigProperty(JoinerConfig.CONDITION_EXPR);
        }
      }
      throw collector.getOrThrowException();
    }
  }

  @Override
  public void prepareRun(BatchJoinerContext context) {
    if (conf.getNumPartitions() != null) {
      context.setNumPartitions(conf.getNumPartitions());
      if (conf.getDistributionFactor() != null && conf.getDistributionFactor() < conf.getNumPartitions()) {
        LOG.warn("Number of partitions ({}) should be greater than or equal to distribution factor ({}) for optimal "
                   + "results.", conf.getNumPartitions(), conf.getDistributionFactor());
      }
    }
    FailureCollector collector = context.getFailureCollector();
    JoinCondition.Op conditionType = conf.getCondition(collector).getOp();
    Set<JoinKey> keys = conditionType == JoinCondition.Op.KEY_EQUALITY ?
      conf.getJoinKeys(collector) : Collections.emptySet();
    context.record(createFieldOperations(conf.getSelectedFields(collector), keys));
  }

  /**
   * Create the field operations from the provided OutputFieldInfo instances and join keys. For join we record several
   * types of transformation; Join, Identity, and Rename. For each of these transformations, if the input field is
   * directly coming from the schema of one of the stage, the field is added as {@code stage_name.field_name}. We keep
   * track of fields outputted by operation (in {@code outputsSoFar set}, so that any operation uses that field as input
   * later, we add it without the stage name.
   * <p>
   * Join transform operation is added with join keys as input tagged with the stage name, and join keys without stage
   * name as output.
   * <p>
   * For other fields which are not renamed in join, Identity transform is added, while for fields which are renamed
   * Rename transform is added.
   *
   * @param outputFields collection of output fields along with information such as stage name, alias
   * @param joinKeys join keys
   * @return List of field operations
   */
  @VisibleForTesting
  static List<FieldOperation> createFieldOperations(List<JoinField> outputFields, Set<JoinKey> joinKeys) {
    LinkedList<FieldOperation> operations = new LinkedList<>();
    Map<String, List<String>> perStageJoinKeys = joinKeys.stream()
      .collect(Collectors.toMap(JoinKey::getStageName, JoinKey::getFields));

    // Add JOIN operation
    List<String> joinInputs = new ArrayList<>();
    Set<String> joinOutputs = new LinkedHashSet<>();
    for (Map.Entry<String, List<String>> joinKey : perStageJoinKeys.entrySet()) {
      for (String field : joinKey.getValue()) {
        joinInputs.add(joinKey.getKey() + "." + field);
        joinOutputs.add(field);
      }
    }
    FieldOperation joinOperation = new FieldTransformOperation("Join", JOIN_OPERATION_DESCRIPTION, joinInputs,
                                                               new ArrayList<>(joinOutputs));
    operations.add(joinOperation);

    Set<String> outputsSoFar = new HashSet<>(joinOutputs);

    for (JoinField outputField : outputFields) {
      // input field name for the operation will come in from schema if its not outputted so far
      String stagedInputField = outputsSoFar.contains(outputField.getFieldName()) ?
        outputField.getFieldName() : outputField.getStageName() + "." + outputField.getFieldName();

      String outputFieldName = outputField.getAlias() == null ? outputField.getFieldName() : outputField.getAlias();
      if (outputField.getFieldName().equals(outputFieldName)) {
        // Record identity transform when using key equality
        List<String> stageJoinKeys = perStageJoinKeys.get(outputField.getStageName());
        if (stageJoinKeys == null || stageJoinKeys.contains(outputField.getFieldName())) {
          // if the field is part of join key no need to emit the identity transform as it is already taken care
          // by join
          continue;
        }
        String operationName = String.format("Identity %s", stagedInputField);
        FieldOperation identity = new FieldTransformOperation(operationName, IDENTITY_OPERATION_DESCRIPTION,
                                                              Collections.singletonList(stagedInputField),
                                                              outputFieldName);
        operations.add(identity);
        continue;
      }

      String operationName = String.format("Rename %s", stagedInputField);

      FieldOperation transform = new FieldTransformOperation(operationName, RENAME_OPERATION_DESCRIPTION,
                                                             Collections.singletonList(stagedInputField),
                                                             outputFieldName);
      operations.add(transform);
    }

    return operations;
  }

  /**
   * Reorders join stages so the supplied stage name is always first.
   * @param stages list of input stages
   * @param leftStage stage to move to the first position in the input list.
   */
  protected void reorderJoinStages(List<JoinStage> stages, String leftStage) {
    stages.sort((js1, js2) -> {
      String s1 = js1.getStageName();
      String s2 = js2.getStageName();

      if (!s1.equals(leftStage) && s2.equals(leftStage)) {
        return 1;
      } else if (s1.equals(leftStage) && !s2.equals(leftStage)) {
        return -1;
      } else {
        return 0;
      }
    });
  }
}
