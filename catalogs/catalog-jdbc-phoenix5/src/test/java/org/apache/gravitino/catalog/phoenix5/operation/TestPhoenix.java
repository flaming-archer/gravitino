/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.phoenix5.operation;

import com.google.common.collect.Maps;

import org.apache.gravitino.catalog.jdbc.TestJdbc;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.catalog.phoenix5.converter.PhoenixColumnDefaultValueConverter;
import org.apache.gravitino.catalog.phoenix5.converter.PhoenixExceptionConverter;
import org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.BeforeAll;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import javax.sql.DataSource;

public class TestPhoenix extends TestJdbc {

  protected static TestDatabaseName TEST_DB_NAME;
//  private static final String JDBC_URL = "jdbc:phoenix:127.0.0.1:2181;phoenix.schema.isNamespaceMappingEnabled=true";
  private static final String JDBC_URL = "jdbc:phoenix:127.0.0.1:2181";
  private static final String JDBC_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
  private static final String USERNAME = "USERNAME";
  private static final String PASSWORD = "PASSWORD";

  @BeforeAll
  public static void startup() throws Exception {
    //    ContainerSuite containerSuite = ContainerSuite.getInstance();
    TEST_DB_NAME = TestDatabaseName.PHOENIX_PHOENIX_ABSTRACT_IT;
    //    containerSuite.startPhoenixContainer(TEST_DB_NAME);
    DataSource dataSource = DataSourceUtils.createDataSource(getPhoenixCatalogProperties());

    DATABASE_OPERATIONS = new PhoenixDatabaseOperations();
    TABLE_OPERATIONS = new PhoenixTableOperations();
    JDBC_EXCEPTION_CONVERTER = new PhoenixExceptionConverter();
    DATABASE_OPERATIONS.initialize(dataSource, JDBC_EXCEPTION_CONVERTER, Collections.emptyMap());
    TABLE_OPERATIONS.initialize(
        dataSource,
        JDBC_EXCEPTION_CONVERTER,
        new PhoenixTypeConverter(),
        new PhoenixColumnDefaultValueConverter(),
        Collections.emptyMap());
  }

  protected static Map<String, String> getPhoenixCatalogProperties() throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), JDBC_URL);
    catalogProperties.put(
        JdbcConfig.JDBC_DRIVER.getKey(), JDBC_DRIVER);
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), USERNAME);
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), PASSWORD);

//    catalogProperties.put("phoenix.schema.isNamespaceMappingEnabled", "true");

    return catalogProperties;
  }
}
