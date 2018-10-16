/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.plugin;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.plugin.format.MultiTableDBInputFormat;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.mysql.jdbc.Driver;
import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.DownloadConfig;
import com.wix.mysql.config.MysqldConfig;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.codec.binary.Base64;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.apache.twill.filesystem.Location;
import org.junit.*;
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

import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.config.DownloadConfig.aDownloadConfig;
import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;
import static com.wix.mysql.distribution.Version.v5_7_latest;

/**
 * Unit tests for our plugins.
 */
public class PipelineTestMysql extends HydratorTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();
  public static final int PORT = 13306;
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  private static final String MYSQL_CONNECTION_STRING = "jdbc:mysql://localhost:" + PORT + "/testdb";
  private static EmbeddedMysql mysqld;

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
            OrcOutputFormat.class, Base64.class);

    // add mysql 3rd party plugin
    PluginClass mysqlPlugin = new PluginClass("jdbc", "mysql", "mysql jdbc driver", Class.forName("com.mysql.jdbc.Driver").getName(),
            null, Collections.<String, PluginPropertyField>emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("mysql-jdbc", "1.0.0"),
            parentArtifact,
            Sets.newHashSet(mysqlPlugin), Driver.class);

//    Class.forName("com.mysql.jdbc.Driver");

    MysqldConfig config = aMysqldConfig(v5_7_latest)
            .withPort(PORT)
            .withUser("test_user", "")
            .build();

    DownloadConfig downloadConfig = aDownloadConfig()
            .withCacheDir(System.getProperty("java.io.tmpdir"))
            .build();

    mysqld = anEmbeddedMysql(config, downloadConfig)
            .addSchema("testdb")
            .start();
  }

  @AfterClass
  public static void cleanup() {
    if (mysqld != null) {
      mysqld.stop();
    }
  }

  @Test
  public void testMultiTableDump() throws Exception {

    try (Connection conn = DriverManager.getConnection(MYSQL_CONNECTION_STRING, "test_user", "")) {
      try (Statement stmt = conn.createStatement()) {
        // note that the tables need quotation marks around them; otherwise, hsql creates them in upper case
        stmt.execute("CREATE TABLE `MULTI1` (`ID` int(11) NOT NULL, `NAME` varchar(32) NOT NULL DEFAULT '')");
        stmt.execute("INSERT INTO `MULTI1` VALUES (0, 'samuel'), (1, 'dwayne')");

        stmt.execute("CREATE TABLE `MULTI2` (`NAME` varchar(32) NOT NULL DEFAULT '', `EMAIL` varchar(64))");
        stmt.execute("INSERT INTO `MULTI2` VALUES ('samuel', 'sj@example.com'), ('dwayne', 'rock@j.com')");

        stmt.execute("CREATE TABLE `MULTI3` (`ITEM` varchar(32) NOT NULL DEFAULT '', `CODE` int(11))");
        stmt.execute("INSERT INTO `MULTI3` VALUES ('donut', 100), ('scotch', 707)");

        stmt.execute("CREATE TABLE `BLACKLIST1` (`ITEM` varchar(32) NOT NULL DEFAULT '', `CODE` int(11))");
        stmt.execute("INSERT INTO `BLACKLIST1` VALUES ('orange', 100), ('newblack', 707)");

        stmt.execute("CREATE TABLE `BLACKLIST2` (`ITEM` varchar(32) NOT NULL DEFAULT '', `CODE` int(11))");
        stmt.execute("INSERT INTO `BLACKLIST2` VALUES ('black', 100), ('mirror', 707)");

      }
    }

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
            .addStage(new ETLStage("source", new ETLPlugin("MultiTableDatabase", BatchSource.PLUGIN_TYPE,
                    ImmutableMap.<String, String>builder()
                            .put("connectionString", MYSQL_CONNECTION_STRING)
                            .put("user", "test_user")
                            .put("jdbcPluginName", "mysql")
                            .put("referenceName", "seequol")
                            .put("schemaNamePattern", "testdb")
                            .put("blackList", "BLACKLIST1,BLACKLIST2")
                            .put("whiteList", "MULTI1,MULTI2,MULTI3")
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

    Assert.assertEquals(ImmutableSet.of("0,samuel", "1,dwayne"), getLines(multi1Manager.get(), partitionKey));
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
            StructuredRecord.builder(schema2).set("NAME", "samuel").set("EMAIL", "sj@example.com")
                    .set("tablename", "MULTI2").build(),
            StructuredRecord.builder(schema2).set("NAME", "dwayne").set("EMAIL", "rock@j.com")
                    .set("tablename", "MULTI2").build(),
            StructuredRecord.builder(schema3).set("ITEM", "donut").set("CODE", 100).set("tablename", "MULTI3").build(),
            StructuredRecord.builder(schema3).set("ITEM", "scotch").set("CODE", 707).set("tablename", "MULTI3").build());
    Assert.assertEquals(actual.size(), outputRecords.size());
    Assert.assertTrue(expected.containsAll(actual));
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
