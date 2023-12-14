// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.catalog.Catalog;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.starrocks.connector.iceberg.IcebergConnector.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergConnector.ICEBERG_CATALOG_TYPE;

public class IcebergHiveCatalogTest {

    @Test
    public void testListAllDatabases(@Mocked IcebergHiveCatalog hiveCatalog) {
        new Expectations() {
            {
                hiveCatalog.listAllDatabases();
                result = Arrays.asList("db1", "db2");
                minTimes = 0;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put("hive.metastore.uris", "thrift://129.1.2.3:9876");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(
                "hive_native_catalog", new Configuration(), icebergProperties);
        List<String> dbs = icebergHiveCatalog.listAllDatabases();
    }

    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG hive_catalog PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://10.201.0.86:32087\")";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);
        createCatalog = "CREATE EXTERNAL CATALOG iceberg_catalog_hms\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"type\" = \"iceberg\",\n" +
                "    \"iceberg.catalog.type\" = \"hive\",\n" +
                "    \"hive.metastore.uris\" = \"thrift://10.201.0.86:32087\",\n" +
                "    \"dlink.catalog.name\"='acc_37'\n" +
                ")";
        starRocksAssert.withCatalog(createCatalog);

        ctx = new ConnectContext(null);
        ctx.setQueryId(UUID.randomUUID());
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
    }

    @Test
    public void useCatalog() throws Exception{
        String sql = "SHOW CATALOGS";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof ShowCatalogsStmt);

        ShowExecutor executor = new ShowExecutor(ctx, (ShowCatalogsStmt) stmt);
        ShowResultSet resultSet = executor.execute();
        resultSet.getResultRows().forEach(System.out::println);

        stmt = AnalyzeTestUtil.analyzeSuccess("set catalog iceberg_catalog_hms");
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, stmt);
        stmtExecutor.execute();

        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://10.201.0.86:32087");
        config.put(ICEBERG_CATALOG_TYPE, "hive");

        Configuration conf = new Configuration();
//        conf.set("aws.s3.use_instance_profile", "false");
//        conf.set("aws.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
//        conf.set("aws.s3.enable_ssl", "false");
//        conf.set("aws.s3.enable_path_style_access", "true");
//        conf.set("aws.s3.region", "us-west-2");
//        conf.set("aws.s3.endpoint", "http://10.201.0.190:31001");
//        conf.set("aws.s3.access_key", "admin");
//        conf.set("aws.s3.secret_key", "adminabcd1234");

        conf.set("fs.s3a.region", "us-west-2");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.endpoint", "http://10.201.0.190:31001");
        conf.set("fs.s3a.access.key", "admin");
        conf.set("fs.s3a.secret.key", "adminabcd1234");

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", conf, config);
        IcebergMetadata metadata = new IcebergMetadata("hive_catalog", new HdfsEnvironment(), icebergHiveCatalog);
        metadata.createDb("fydb", ImmutableMap.of("location", "s3a://dlink/fydb"));


        stmt = AnalyzeTestUtil.analyzeSuccess("show databases");
        //        stmt = (ShowDbStmt) UtFrameUtils.parseStmtWithNewParser("show databases", ctx);
        executor = new ShowExecutor(ctx, (ShowStmt) stmt);
        resultSet = executor.execute();
        System.out.println("-------------");
        resultSet.getResultRows().forEach(System.out::println);

    }

    @Test
    public void testIcebergMetadata() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://10.201.0.86:32087");
        config.put(ICEBERG_CATALOG_TYPE, "hive");

        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-west-2");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.endpoint", "http://10.201.0.190:31001");
        conf.set("fs.s3a.access.key", "admin");
        conf.set("fs.s3a.secret.key", "adminabcd1234");
        conf.set("metastore.catalog.default", "acc_37");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", conf, config);
        IcebergMetadata metadata = new IcebergMetadata("hive_catalog", new HdfsEnvironment(), icebergHiveCatalog);
        String dbName = "fydb";
        if (!metadata.dbExists(dbName)) {
//            metadata.dropDb(dbName, true);
            metadata.createDb(dbName, ImmutableMap.of("location", "s3a://dlink/fydb"));
        }
        metadata.listDbNames().forEach(System.out::println);
    }

    @Test
    public void myListAllDatabases() {
        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put("hive.metastore.uris", "thrift://10.201.0.86:32087");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(
                "hive_native_catalog", new Configuration(), icebergProperties);
        List<String> dbs = icebergHiveCatalog.listAllDatabases("acc_37");
        dbs.forEach(System.out::println);
        Assert.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }
}
