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

package io.cdap.plugin;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.PartitionDetail;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.format.MultiTableDBInputFormat;
import io.cdap.plugin.format.error.collector.ErrorCollectingMultiTableDBInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.codec.binary.Base64;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.apache.twill.filesystem.Location;
import org.hsqldb.Server;
import org.hsqldb.jdbc.JDBCDriver;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for our plugins.
 */
public class PipelineTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  private static final String CONNECTION_STRING = "jdbc:hsqldb:hsql://localhost/testdb";

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static Server server;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      MultiTableDBSource.class, MultiTableDBInputFormat.class, AvroKeyOutputFormat.class,
                      ErrorCollectingMultiTableDBInputFormat.class,
                      OrcOutputFormat.class, Base64.class);

    // add hypersql 3rd party plugin
    PluginClass hypersql = new PluginClass("jdbc", "hypersql", "hypersql jdbc driver", JDBCDriver.class.getName(),
                                           null, Collections.<String, PluginPropertyField>emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("hsql-jdbc", "1.0.0"),
                      parentArtifact,
                      Sets.newHashSet(hypersql), JDBCDriver.class);

    Class.forName(JDBCDriver.class.getName());

    String hsqlDBDir = temporaryFolder.newFolder("hsqldb").getAbsolutePath();
    server = new Server();
    server.setDatabasePath(0, hsqlDBDir + "/testdb");
    server.setDatabaseName(0, "testdb");
    server.start();
  }

  @AfterClass
  public static void cleanup() {
    server.stop();
  }

  @Test
  public void testMultiTableDump() throws Exception {
    Connection conn = DriverManager.getConnection(CONNECTION_STRING);
    try (Statement stmt = conn.createStatement()) {
      // note that the tables need quotation marks around them; otherwise, hsql creates them in upper case
      stmt.execute("CREATE TABLE \"MULTI1\" (ID INT NOT NULL, NAME VARCHAR(32) NOT NULL, PRIMARY KEY (ID))");
      stmt.execute("INSERT INTO \"MULTI1\" VALUES (0, 'samuel'), (1, 'dwayne'), (2, 'dwayne2')");

      stmt.execute("CREATE TABLE \"MULTI2\" (NAME VARCHAR(32) NOT NULL, EMAIL VARCHAR(64))");
      stmt.execute("INSERT INTO \"MULTI2\" VALUES ('samuel', 'sj@example.com'), ('dwayne', 'rock@j.com')");

      stmt.execute("CREATE TABLE \"MULTI3\" (ITEM VARCHAR(32) NOT NULL, CODE INT)");
      stmt.execute("INSERT INTO \"MULTI3\" VALUES ('donut', 100), ('scotch', 707)");

      stmt.execute("CREATE TABLE \"BLACKLIST1\" (ITEM VARCHAR(32) NOT NULL, CODE INT)");
      stmt.execute("INSERT INTO \"BLACKLIST1\" VALUES ('orange', 100), ('newblack', 707)");

      stmt.execute("CREATE TABLE \"BLACKLIST2\" (ITEM VARCHAR(32) NOT NULL, CODE INT)");
      stmt.execute("INSERT INTO \"BLACKLIST2\" VALUES ('black', 100), ('mirror', 707)");

    }

    ETLBatchConfig config = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("source", new ETLPlugin("MultiTableDatabase", BatchSource.PLUGIN_TYPE,
                                                     ImmutableMap.<String, String>builder()
                                                       .put("connectionString", CONNECTION_STRING)
                                                       .put("jdbcPluginName", "hypersql")
                                                       .put("referenceName", "seequol")
                                                       .put("blackList", "BLACKLIST1,BLACKLIST2")
                                                       .put("whiteList", "MULTI1,MULTI2,MULTI3")
                                                       .put("dataSelectionMode", "allow-list")
                                                       .put("splitsPerTable", "2")
                                                       .build())))
      .addStage(new ETLStage("sink1", MockSink.getPlugin("multiOutput")))
      .addStage(new ETLStage("sink2", new ETLPlugin("DynamicMultiFileset", BatchSink.PLUGIN_TYPE,
                                                    ImmutableMap.of("delimiter", ","))))
      .addConnection("source", "sink1")
      .addConnection("source", "sink2")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("multitable");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    long logicalStart = System.currentTimeMillis();
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(ImmutableMap.of("logical.start.time", String.valueOf(logicalStart)));
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check multi fileset sink
    DataSetManager<PartitionedFileSet> multi1Manager = getDataset("MULTI1");
    DataSetManager<PartitionedFileSet> multi2Manager = getDataset("MULTI2");
    DataSetManager<PartitionedFileSet> multi3Manager = getDataset("MULTI3");
    DataSetManager<PartitionedFileSet> blacklist1 = getDataset("BLACKLIST1");
    DataSetManager<PartitionedFileSet> blacklist2 = getDataset("BLACKLIST2");

    Assert.assertNull(blacklist1.get());
    Assert.assertNull(blacklist2.get());


    long timePartition = TimeUnit.SECONDS.convert(logicalStart, TimeUnit.MILLISECONDS);
    PartitionKey partitionKey = PartitionKey.builder().addLongField("ingesttime", timePartition).build();

    Assert.assertEquals(ImmutableSet.of("0,samuel", "1,dwayne", "2,dwayne2"), getLines(multi1Manager.get(),
                                                                                       partitionKey));
    Assert.assertEquals(ImmutableSet.of("samuel,sj@example.com", "dwayne,rock@j.com"),
                        getLines(multi2Manager.get(), partitionKey));
    Assert.assertEquals(ImmutableSet.of("donut,100", "scotch,707"), getLines(multi3Manager.get(), partitionKey));

    // check raw output
    Schema schema1 = Schema.recordOf(
      "MULTI1",
      Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
      Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("tablename", Schema.of(Schema.Type.STRING)));
    Schema schema2 = Schema.recordOf(
      "MULTI2",
      Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("EMAIL", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("tablename", Schema.of(Schema.Type.STRING)));
    Schema schema3 = Schema.recordOf(
      "MULTI3",
      Schema.Field.of("ITEM", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("CODE", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("tablename", Schema.of(Schema.Type.STRING)));

    DataSetManager<Table> outputManager = getDataset("multiOutput");
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Set<StructuredRecord> actual = new HashSet<>(outputRecords);
    Set<StructuredRecord> expected = ImmutableSet.of(
      StructuredRecord.builder(schema1).set("ID", 0).set("NAME", "samuel").set("tablename", "MULTI1").build(),
      StructuredRecord.builder(schema1).set("ID", 1).set("NAME", "dwayne").set("tablename", "MULTI1").build(),
      StructuredRecord.builder(schema1).set("ID", 2).set("NAME", "dwayne2").set("tablename", "MULTI1").build(),
      StructuredRecord.builder(schema2).set("NAME", "samuel").set("EMAIL", "sj@example.com")
        .set("tablename", "MULTI2").build(),
      StructuredRecord.builder(schema2).set("NAME", "dwayne").set("EMAIL", "rock@j.com")
        .set("tablename", "MULTI2").build(),
      StructuredRecord.builder(schema3).set("ITEM", "donut").set("CODE", 100).set("tablename", "MULTI3").build(),
      StructuredRecord.builder(schema3).set("ITEM", "scotch").set("CODE", 707).set("tablename", "MULTI3").build());
    Assert.assertEquals(actual.size(), outputRecords.size());
    Assert.assertEquals(expected, actual);
  }

  private Set<String> getLines(PartitionedFileSet fileset, PartitionKey partitionKey) throws IOException {
    PartitionDetail partitionDetail = fileset.getPartition(partitionKey);
    Location location = partitionDetail.getLocation();
    Set<String> lines = new HashSet<>();
    for (Location file : location.list()) {
      String fileName = file.getName();
      if (fileName.endsWith(".crc") || fileName.startsWith(".")) {
        continue;
      }
      try (BufferedReader reader =
             new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          lines.add(line);
        }
      }
    }
    return lines;
  }
}
