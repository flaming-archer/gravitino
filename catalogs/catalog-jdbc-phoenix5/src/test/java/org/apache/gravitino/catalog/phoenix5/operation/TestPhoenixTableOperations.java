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


import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tag("gravitino-docker-test")
public class TestPhoenixTableOperations extends TestPhoenix {

  private static final Type STRING = Types.StringType.get();
  private static final Type INT = Types.IntegerType.get();
  //  private static final Type LONG = Types.LongType.get();

  @Test
  public void testCreateAllTypeTable() {
    String tableName = RandomNameUtils.genRandomName("type_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("COL_1")
            .withType(Types.ByteType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("COL_2")
            .withType(Types.ShortType.get())
            .withNullable(true)
            .build());
    columns.add(JdbcColumn.builder().withName("COL_3").withType(INT).withNullable(true).build());
    columns.add(
        JdbcColumn.builder()
            .withName("COL_4")
            .withType(Types.LongType.get())
            .withNullable(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("COL_5")
            .withType(Types.FloatType.get())
            .withNullable(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("COL_6")
            .withType(Types.DoubleType.get())
            .withNullable(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("COL_7")
            .withType(Types.DateType.get())
            .withNullable(true)
            .build());
    //    columns.add(
    //        JdbcColumn.builder()
    //            .withName("col_8")
    //            .withType(Types.TimeType.get())
    //            .withNullable(false)
    //            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("COL_9")
            .withType(Types.TimestampType.withoutTimeZone())
            .withNullable(true)
            .build());
    columns.add(
        JdbcColumn.builder().withName("COL_10").withType(Types.DecimalType.of(10, 2)).build());
    columns.add(
        JdbcColumn.builder().withName("COL_11").withType(STRING).withNullable(true).build());
    columns.add(
        JdbcColumn.builder()
            .withName("COL_12")
            .withType(Types.FixedCharType.of(10))
            .withNullable(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("COL_13")
            .withType(Types.StringType.get())
            .withNullable(true)
            .build());
    //    columns.add(
    //        JdbcColumn.builder()
    //            .withName("col_14")
    //            .withType(Types.BinaryType.get())
    //            .withNullable(true)
    //            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("COL_15")
            .withType(Types.FixedCharType.of(10))
            .withNullable(true)
            .build());

//    columns.add(
//        JdbcColumn.builder()
//            .withName("COL_16.FOR")
//            .withType(Types.FixedCharType.of(10))
//            .withNullable(true)
//            .build());
//
//    columns.add(
//        JdbcColumn.builder()
//            .withName("COL_16.BAR")
//            .withType(Types.FixedCharType.of(10))
//            .withNullable(true)
//            .build());

    // Test create increment key for unique index.
    Index[] indexes =
        new Index[]{
            Indexes.primary("PK_COL_1", new String[][]{{"col_1"}})
        };

    Map<String, String> properties = new HashMap<>();
    String comment = null;
    DATABASE_OPERATIONS.create(TEST_DB_NAME.toString(), comment, properties);

    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME.toString(),
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        Collections.emptyMap(),
        null,
        Distributions.NONE,
        indexes,
        null);

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), tableName.toUpperCase());

    assertionsTableInfo(
        tableName.toUpperCase(),
        null,
        columns,
        Collections.emptyMap(),
        null,
        Transforms.EMPTY_TRANSFORM,
        load);
  }

  @Test
  public void testListTables() {

    System.out.println(TABLE_OPERATIONS.listTables(TEST_DB_NAME.toString().toUpperCase()));
  }
}
