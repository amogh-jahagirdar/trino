/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.assertj.core.util.Files;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class RestCatalogTestUtils
{
    private RestCatalogTestUtils() {}

    static class TestingJdbcCatalog
            extends JdbcCatalog implements ViewCatalog
    {
        InMemoryCatalog viewCatalog;

        public TestingJdbcCatalog()
        {
            this((Function) null, (Function) null, true);
        }

        public TestingJdbcCatalog(
                Function<Map<String, String>, FileIO> ioBuilder,
                Function<Map<String, String>, JdbcClientPool> clientPoolBuilder,
                boolean initializeCatalogTables)
        {
            super(ioBuilder, clientPoolBuilder, initializeCatalogTables);
            this.viewCatalog = new InMemoryCatalog();
            viewCatalog.initialize("testing-views", ImmutableMap.of());
        }

        @Override
        public List<TableIdentifier> listViews(Namespace namespace)
        {
            return viewCatalog.listViews(namespace);
        }

        @Override
        public View loadView(TableIdentifier identifier)
        {
            return viewCatalog.loadView(identifier);
        }

        @Override
        public ViewBuilder buildView(TableIdentifier identifier)
        {
            return viewCatalog.buildView(identifier);
        }

        @Override
        public boolean dropView(TableIdentifier identifier)
        {
            return viewCatalog.dropView(identifier);
        }

        @Override
        public void renameView(TableIdentifier from, TableIdentifier to)
        {
            viewCatalog.renameView(from, to);
        }
    }

    public static Catalog backendCatalog(File warehouseLocation)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(CatalogProperties.URI, "jdbc:h2:file:" + Files.newTemporaryFile().getAbsolutePath());
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation.toPath().resolve("iceberg_data").toFile().getAbsolutePath());

        ConnectorSession connectorSession = TestingConnectorSession.builder().build();
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        HdfsContext context = new HdfsContext(connectorSession);

        JdbcCatalog catalog = new TestingJdbcCatalog();
        catalog.setConf(hdfsEnvironment.getConfiguration(context, new Path(warehouseLocation.getAbsolutePath())));
        catalog.initialize("backend_jdbc", properties.buildOrThrow());

        return catalog;
    }
}
