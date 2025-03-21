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
package org.apache.gravitino.catalog.gbase.operation;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.base.Preconditions;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Table operations for Gbase.
 */
public class GbaseTableOperations extends JdbcTableOperations {


  @Override
  public void create(
      String databaseName,
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes,
      SortOrder[] sortOrders)
      throws TableAlreadyExistsException {
    LOG.info("Attempting to create table {} in database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(
          connection,
          generateCreateTableSql(
              databaseName,
              tableName,
              columns,
              comment,
              properties,
              partitioning,
              distribution,
              indexes,
              sortOrders));
      LOG.info("Created table {} in database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes) {
    throw new UnsupportedOperationException(
        "generateCreateTableSql with out sortOrders in gbase is not supported");
  }

  protected String generateCreateTableSql(
      String databaseName,
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes,
      SortOrder[] sortOrders) {
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException("Currently we do not support Partitioning in mysql");
    }

    Preconditions.checkArgument(
        Distributions.NONE.equals(distribution),
        "Currently we do not support distribution in mysql");

    validateIncrementCol(columns, indexes);
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("CREATE TABLE ")
        .append(tableName)
        .append(" (\n");

    // Add columns
    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sqlBuilder
          .append(SPACE)
          .append(SPACE)
          .append(column.name());

      appendColumnDefinition(column, sqlBuilder);
      // Add a comma for the next column, unless it's the last one
      if (i < columns.length - 1) {
        sqlBuilder.append(",\n");
      }
    }

    appendIndexesSql(indexes, sqlBuilder);

    sqlBuilder.append("\n)");

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder.append(" COMMENT='").append(comment).append("'");
    }

    // Add table properties if any
    if (MapUtils.isNotEmpty(properties)) {
      sqlBuilder.append(
          properties.entrySet().stream()
              .map(entry -> String.format("%s = %s", entry.getKey(), entry.getValue()))
              .collect(Collectors.joining(",\n", "\n", "")));
    }

    // Return the generated SQL statement
    String result = sqlBuilder.append(";").toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  public static void appendIndexesSql(Index[] indexes, StringBuilder sqlBuilder) {
    if (indexes == null) {
      return;
    }

    for (Index index : indexes) {
      String fieldStr = getIndexFieldStr(index.fieldNames());
      sqlBuilder.append(",\n");
      switch (index.type()) {
        case PRIMARY_KEY:
          sqlBuilder.append("PRIMARY KEY ")
              .append("(").append(fieldStr).append(")");
          break;
        default:
          throw new IllegalArgumentException(
              "Gravitino gbase doesn't support index : " + index.type());
      }
    }
  }

  protected static String getIndexFieldStr(String[][] fieldNames) {
    return Arrays.stream(fieldNames)
        .map(
            colNames -> {
              if (colNames.length > 1) {
                throw new IllegalArgumentException(
                    "Index does not support complex fields in this Catalog");
              }
              return String.format("%s", colNames[0]);
            })
        .collect(Collectors.joining(", "));
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "Gbase does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected String generateAlterTableSql(String databaseName, String tableName,
      TableChange... changes) {
    throw new UnsupportedOperationException("alter table is not supported");
  }


  private StringBuilder appendColumnDefinition(JdbcColumn column, StringBuilder sqlBuilder) {
    // Add data type
    sqlBuilder.append(SPACE).append(typeConverter.fromGravitino(column.dataType())).append(SPACE);

    // Add NOT NULL if the column is marked as such
    if (column.nullable()) {
      sqlBuilder.append("NULL ");
    } else {
      sqlBuilder.append("NOT NULL ");
    }

    // Add DEFAULT value if specified
    if (!DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
      sqlBuilder
          .append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(column.defaultValue()))
          .append(SPACE);
    }

    // Add column auto_increment if specified
    if (column.autoIncrement()) {
      sqlBuilder.append("AUTO_INCREMENT").append(" ");
    }

    // Add column comment if specified
    if (StringUtils.isNotEmpty(column.comment())) {
      sqlBuilder.append("COMMENT '").append(column.comment()).append("' ");
    }
    return sqlBuilder;
  }


}
