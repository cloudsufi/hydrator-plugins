/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.plugin.common.stepsdesign;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cucumber.java.en.Then;
import org.apache.directory.api.util.Strings;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stepsdesign.BeforeActions;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *  Core Plugin Common Step Design.
 */
public class CorePlugin implements CdfHelper {
  private static final Logger LOG = LoggerFactory.getLogger(CorePlugin.class);
  @Then("Verify the CSV Output File matches the Expected Output File: {string} With Expected Partitions: {string}")
  public void verifyCSVOutput(String file, String expectedPartitions) {
    String gcsTargetBucket =  PluginPropertyUtils.pluginProp("gcsTargetBucket");
    try {
      // The output gcs folder will be like:
      // e2e-test-[uuid]
      // --2022-06-26-00-27/
      // ----_SUCCESS
      // ----part-r-0000
      // ----part-r-0001
      // ----part-r-...
      // The number of part-r-* files should match the expected partitions.
      int partitions = 0;
      List<String> lst = new ArrayList<>();
      for (Blob blob : StorageClient.listObjects(gcsTargetBucket).iterateAll()) {
        String name = blob.getName();
        if (name.contains("part-r")) {
          partitions++;
          try (InputStream inputStream = new ByteArrayInputStream(blob.getContent())) {
            readInputStream(inputStream, lst);
          }
        }
      }
      Path path = Paths.get(Objects.requireNonNull(CorePlugin.class.getResource
        ("/" + PluginPropertyUtils.pluginProp(file))).getPath()).toAbsolutePath();
      if (Integer.parseInt(expectedPartitions) != 0) {
        Assert.assertEquals("Output partition should match",
                            partitions, Integer.parseInt(PluginPropertyUtils.pluginProp(expectedPartitions)));
      }
      Assert.assertTrue("Output content should match",
                        Strings.equals(getSortedCSVContent(lst), new String(Files.readAllBytes(path))));
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        BeforeActions.scenario.write("GCS Bucket " + gcsTargetBucket + " does not exist.");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }
  private String getSortedCSVContent(List<String> lst) {
    // Since the spark output files aren't guaranteed to be ordered, the output entry with [id, field1, field2, ....]
    // schema needs to be sorted by id for comparison purpose.
    lst.sort((s1, s2) -> {
      String id1 = s1.split(",")[0], id2 = s2.split(",")[0];
      return Integer.parseInt(id1) - Integer.parseInt(id2);
    });
    StringBuilder sb = new StringBuilder();
    for (String s : lst) {
      sb.append(s);
    }
    return sb.toString();
  }

  private void readInputStream(InputStream input, List<String> lst) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lst.add(line + "\n");
      }
    }
  }

  @Then("Validate output file generated by file sink plugin {string} is equal to expected output file {string}")
  public static void validateOutputFileGeneratedByFileSinkPluginIsEqualToExpectedOutputFile(String fileSinkBucketPath,
    String expectedOutputFilePath) {
    if (TestSetupHooks.testOnCdap.equals("true")) {
      ValidationHelper.compareData(fileSinkBucketPath, PluginPropertyUtils.pluginProp(expectedOutputFilePath));
    } else {
      String outputFileSinkBucketName = PluginPropertyUtils.pluginProp(fileSinkBucketPath);
      ValidationHelper.listBucketObjects(outputFileSinkBucketName.substring(5),
        PluginPropertyUtils.pluginProp(expectedOutputFilePath));
    }
  }

  @Then("Validate output file generated by xmlReader and file sink plugin {string} is equal to expected" +
    " output file {string}")
  public static void validateOutputFileGeneratedByFileSinkPluginIsEqualToExpectedOutputFileForXml
    (String fileSinkBucketPath, String expectedOutputFilePath) {
    if (TestSetupHooks.testOnCdap.equals("true")) {
      String bucketName = TestSetupHooks.fileSourceBucket;
      ValidationHelper.compareDataForXml(fileSinkBucketPath,
                                         PluginPropertyUtils.pluginProp(expectedOutputFilePath), bucketName);
    } else {
      String outputFileSinkBucketName = PluginPropertyUtils.pluginProp(fileSinkBucketPath);
      ValidationHelper.listBucketObjectsForXml(outputFileSinkBucketName.substring(5),
                                         PluginPropertyUtils.pluginProp(expectedOutputFilePath));
    }
  }

  @Then("Validate output Orc file generated by file sink plugin {string} is equal to expected output file {string}")
  public void validateOutputOrcFileGeneratedByFileSinkPluginIsEqualToExpectedOutputFile(String fileSinkBucketPath,
    String expectedOutputFilePath) throws IOException {
    if (TestSetupHooks.testOnCdap.equals("true")) {
      ValidationHelper.compareDataOfOrcFiles(PluginPropertyUtils.pluginProp(expectedOutputFilePath));
    } else {
      String outputFileSinkBucketName = PluginPropertyUtils.pluginProp(fileSinkBucketPath);
      ValidationHelper.listBucketObjects(outputFileSinkBucketName.substring(5),
        PluginPropertyUtils.pluginProp(expectedOutputFilePath));
    }
  }

  @Then("Validate that file gets successfully deleted from the gcs bucket")
  public static boolean validateThatFileGetsDeletedFromTheGcsBucket() {
    String bucketName = TestSetupHooks.fileSourceBucket;
    String fileName = PluginPropertyUtils.pluginProp("xmlFileName");
      // Instantiate a client for Google Cloud Storage
    Storage storage = StorageOptions.newBuilder().setProjectId(PluginPropertyUtils.pluginProp("projectId"))
      .build().getService();
      // Check if the file exists in the bucket
      Blob blob = storage.get(bucketName, fileName);
      // If blob is null, the file does not exist
    boolean isDeleted = (blob == null);
    if (isDeleted) {
      LOG.info("The file " + fileName + " has been successfully deleted from the bucket " + bucketName + ".");
    } else {
      LOG.info("The file " + fileName + " still exists in the bucket " + bucketName + ".");
    }
    return isDeleted;
  }

  @Then("Validate that file gets successfully moved to the target location")
  public static boolean verifyFileMovedWithinGCSBucket() {
    // Instantiate a client for Google Cloud Storage with the specified project ID
    Storage storage = StorageOptions.newBuilder().setProjectId(PluginPropertyUtils.pluginProp("projectId"))
      .build().getService();
    String bucketName = TestSetupHooks.fileSourceBucket;
    String fileName = PluginPropertyUtils.pluginProp("xmlFileName");
    String targetLocation = PluginPropertyUtils.pluginProp("bucketName");
    // Check if the source file exists
    Blob sourceBlob = storage.get(bucketName, fileName);

    // Check if the target file exists
    Blob targetBlob = storage.get(targetLocation, fileName);

    // Verify the file has been moved by checking if the source file does not exist and the target file exists
    boolean isMoved = sourceBlob == null && targetBlob != null;
    if (isMoved) {
      LOG.info("The file " + fileName + " was successfully moved to target location in the bucket ");
    } else if (sourceBlob != null) {
      LOG.info("The source file " + fileName + " still exists in the bucket " + bucketName + ".");
    } else {
      LOG.info("The target file " + fileName + " does not exist in the bucket " + bucketName + ".");
    }
    return isMoved;
  }
}
