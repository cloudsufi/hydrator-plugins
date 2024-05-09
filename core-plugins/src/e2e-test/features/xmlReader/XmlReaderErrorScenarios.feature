# Copyright © 2024 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

@XmlReader_Source
Feature:File Sink - Verify XML Reader Plugin Error scenarios

  @XMLREADER_DELETE_TEST @FILE_SINK_TEST
  Scenario: Verify Pipeline fails when an invalid pattern is entered
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "XML Reader" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "XMLReader" and "File" to establish connection
    Then Navigate to the properties page of plugin: "XMLReader"
    Then Enter input plugin property: "referenceName" with value: "ReferenceName"
    Then Enter input plugin property: "path" with value: "xmlTestFile"
    Then Enter input plugin property: "nodePath" with value: "node"
    Then Select dropdown plugin property: "reprocessingRequired" with option value: "No"
    Then Enter input plugin property: "pattern" with value: "invalidPattern"
    Then Validate "XMLReader" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "fileSinkTargetBucket"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Failed"
    Then Close the pipeline logs

  @XMLREADER_TEST @FILE_SINK_TEST
  Scenario: Verify no data is transferred when an invalid node path is entered
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "XML Reader" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "File" from the plugins list as: "Sink"
    Then Connect plugins: "XMLReader" and "File" to establish connection
    Then Navigate to the properties page of plugin: "XMLReader"
    Then Enter input plugin property: "referenceName" with value: "ReferenceName"
    Then Enter input plugin property: "path" with value: "xmlTestFile"
    Then Enter input plugin property: "nodePath" with value: "invalidNode"
    Then Select dropdown plugin property: "reprocessingRequired" with option value: "No"
    Then Validate "XMLReader" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "File"
    Then Enter input plugin property: "referenceName" with value: "FileReferenceName"
    Then Enter input plugin property: "path" with value: "fileSinkTargetBucket"
    Then Replace input plugin property: "pathSuffix" with value: "yyyy-MM-dd-HH-mm"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Validate "File" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count is equal to IN record count
