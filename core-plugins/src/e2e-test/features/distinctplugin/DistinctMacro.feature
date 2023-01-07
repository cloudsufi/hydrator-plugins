@Distinct
Feature: Distinct analytics - Verify File data transfer scenarios using Distinct analytics

  @GCS_DISTINCT_TEST2 @FILE_SINK_TEST
  Scenario: To verify data is getting transferred from File source to File sink with properties as macro arguments
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Distinct" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Distinct" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "Distinct" and "File2" to establish connection
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "gcsDistinctTest2"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "distinctCsvFileSchema"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Distinct"
    Then Click on the Macro button of Property: "fields" and set the value to: "DistinctFieldName"
    Then Validate "Distinct" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File2"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "fileSinkTargetBucket"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm-ss"
    Then Select dropdown plugin property: "format" with option value: "tsv"
    Then Validate "File2" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "distinctValidMultipleFieldsNames" for key "DistinctFieldName"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "distinctValidMultipleFieldsNames" for key "DistinctFieldName"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count of distinct is equal to IN record count of sink
#    Then Validate output records in output folder path "filePluginOutputFolder" is equal to expected output file "distinctMacroOutputFile"
