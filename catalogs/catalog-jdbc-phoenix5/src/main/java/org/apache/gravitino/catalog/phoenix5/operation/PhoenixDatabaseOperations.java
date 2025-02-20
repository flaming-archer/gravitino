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

import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.MapUtils;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Database operations for Phoenix.
 */
public class PhoenixDatabaseOperations extends JdbcDatabaseOperations {

  @Override
  protected boolean supportSchemaComment() {
    return false;
  }

  @Override
  protected Set<String> createSysDatabaseNameSet() {
    return ImmutableSet.of("SYSTEM", "default", "hbase");
  }

  @Override
  protected String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {
    String createDatabaseSql = String.format("CREATE SCHEMA IF NOT EXISTS %s ", databaseName);
    if (MapUtils.isNotEmpty(properties)) {
      throw new UnsupportedOperationException("Properties are not supported yet.");
    }
    LOG.info("Generated create database:{} sql: {}", databaseName, createDatabaseSql);
    return createDatabaseSql;
  }

  @Override
  public List<String> listDatabases() {
    Set<String> databaseNames = new HashSet<>();
    try (final Connection connection = getConnection()) {
      // It is possible that other catalogs have been deleted,
      // causing the following statement to error,
      // so here we manually set a system catalog
      //      connection.setCatalog(createSysDatabaseNameSet().iterator().next());
      try (Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery(
              "SELECT TABLE_SCHEM FROM SYSTEM.CATALOG ")) {
        while (resultSet.next()) {
          String databaseName = resultSet.getString(1);
          if (!isSystemDatabase(databaseName)) {
            databaseNames.add(databaseName);
          }
        }
      }
      return databaseNames.stream().filter(Objects::nonNull).collect(Collectors.toList());
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  protected boolean isSystemDatabase(String dbName) {
    return createSysDatabaseNameSet().contains(dbName);
  }

  protected String generateDropDatabaseSql(String databaseName, boolean cascade) {
    final String dropDatabaseSql = String.format("DROP SCHEMA %s", databaseName);
    if (cascade) {
      return dropDatabaseSql;
    }

    try (final Connection connection = this.dataSource.getConnection()) {
      String query = String.format("SHOW TABLES IN `%s`", databaseName);
      try (Statement statement = connection.createStatement()) {
        // Execute the query and check if there exists any tables in the database
        try (ResultSet resultSet = statement.executeQuery(query)) {
          if (resultSet.next()) {
            throw new IllegalStateException(
                String.format(
                    "Database %s is not empty, the value of cascade should be true.",
                    databaseName));
          }
        }
      }
    } catch (SQLException sqlException) {
      throw this.exceptionMapper.toGravitinoException(sqlException);
    }
    return dropDatabaseSql;
  }
}
