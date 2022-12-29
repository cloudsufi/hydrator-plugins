@Joiner
Feature: Joiner - Verify File source to File sink data transfer using Joiner analytics

  @JOINER_TEST1 @JOINER_TEST2 @FILE_SINK_TEST
  Scenario:To verify data is getting transferred from File to File successfully using Joiner plugin with outer join type
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Select plugin: "File" from the plugins list as: "Source"
    And Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Joiner" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Joiner" to establish connection
    Then Connect plugins: "File2" and "Joiner" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "Joiner" and "File3" to establish connection
    Then Click plugin property: "alignPlugins" button
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerInputTest1"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvFileFirstSchema"
    Then Validate "File2" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File2"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerInputTest2"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvDataTypeFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "Joiner"
    Then Select joiner type "Outer"
    Then Select radio button plugin property: "conditionType" with value: "basic"
    Then Click on the Get Schema button
    Then Validate "Joiner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File3"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "fileSinkTargetBucket"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm-ss"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count of joiner is equal to IN record count of sink
#    Then Validate output records in output folder path "filePluginOutputFolder" is equal to expected output file "joinerTest1OutputFile"

  @JOINER_TEST1 @JOINER_TEST2 @FILE_SINK_TEST
  Scenario:To verify data is getting transferred from File to File successfully using Joiner plugin with inner join type
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Select plugin: "File" from the plugins list as: "Source"
    And Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Joiner" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Joiner" to establish connection
    Then Connect plugins: "File2" and "Joiner" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "Joiner" and "File3" to establish connection
    Then Click plugin property: "alignPlugins" button
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerInputTest1"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvFileFirstSchema"
    Then Validate "File2" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File2"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerInputTest2"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvDataTypeFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "Joiner"
    Then Select joiner type "Inner"
    Then Select radio button plugin property: "conditionType" with value: "basic"
    Then Click on the Get Schema button
    Then Validate "Joiner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File3"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "fileSinkTargetBucket"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm-ss"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count of joiner is equal to IN record count of sink
#    Then Validate output records in output folder path "filePluginOutputFolder" is equal to expected output file "joinerTest2OutputFile"

  @JOINER_TEST3 @JOINER_TEST4 @FILE_SINK_TEST
  Scenario:To verify data is getting transferred from File to File successfully using Joiner plugin with null keys false
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Select plugin: "File" from the plugins list as: "Source"
    And Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Joiner" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Joiner" to establish connection
    Then Connect plugins: "File2" and "Joiner" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "Joiner" and "File3" to establish connection
    Then Click plugin property: "alignPlugins" button
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerCsvNullFileInputTest1"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvNullFileFirstSchema"
    Then Validate "File2" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File2"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerCsvNullFileInputTest2"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvNullFileSecondSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "Joiner"
    Then Select joiner type "Inner"
    Then Select radio button plugin property: "conditionType" with value: "basic"
    Then Click plugin property: "joinerNullKeys"
    Then Click on the Get Schema button
    Then Validate "Joiner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File3"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "fileSinkTargetBucket"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm-ss"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count of joiner is equal to IN record count of sink
#    Then Validate output records in output folder path "filePluginOutputFolder" is equal to expected output file "joinerTest3OutputFile"

  @JOINER_TEST3 @JOINER_TEST4 @FILE_SINK_TEST
  Scenario:To verify data is getting transferred from File to File successfully using Joiner plugin with null keys true
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Select plugin: "File" from the plugins list as: "Source"
    And Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Joiner" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Joiner" to establish connection
    Then Connect plugins: "File2" and "Joiner" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "Joiner" and "File3" to establish connection
    Then Click plugin property: "alignPlugins" button
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerCsvNullFileInputTest1"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvNullFileFirstSchema"
    Then Validate "File2" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File2"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "joinerCsvNullFileInputTest2"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "joinerCsvNullFileSecondSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "Joiner"
    Then Select joiner type "Outer"
    Then Select radio button plugin property: "conditionType" with value: "basic"
    Then Click on the Get Schema button
    Then Validate "Joiner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File3"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "fileSinkTargetBucket"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm-ss"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count of joiner is equal to IN record count of sink
#    Then Validate output records in output folder path "filePluginOutputFolder" is equal to expected output file "joinerTest4OutputFile"
