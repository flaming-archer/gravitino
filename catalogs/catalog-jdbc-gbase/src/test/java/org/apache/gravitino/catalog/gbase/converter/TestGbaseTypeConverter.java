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
package org.apache.gravitino.catalog.gbase.converter;

import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.BIGINT;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.BLOB;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.CHAR;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.DATE;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.DATETIME;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.DECIMAL;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.DOUBLE;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.FLOAT;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.INT;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.LONGBLOB;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.SMALLINT;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.TEXT;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.TIME;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.TIMESTAMP;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.TINYINT;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.VARCHAR;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link GbaseTypeConverter}
 */
public class TestGbaseTypeConverter {

  private static final GbaseTypeConverter GBASE_TYPE_CONVERTER =
      new GbaseTypeConverter();
  private static final String USER_DEFINED_TYPE = "user-defined";

  @Test
  public void testToGravitinoType() {
    checkJdbcTypeToGravitinoType(Types.ByteType.get(), TINYINT, null, null);
    checkJdbcTypeToGravitinoType(Types.ShortType.get(), SMALLINT, null, null);
    checkJdbcTypeToGravitinoType(Types.IntegerType.get(), INT, null, null);
    checkJdbcTypeToGravitinoType(Types.LongType.get(), BIGINT, null, null);
    checkJdbcTypeToGravitinoType(Types.FloatType.get(), FLOAT, null, null);
    checkJdbcTypeToGravitinoType(Types.DoubleType.get(), DOUBLE, null, null);
    checkJdbcTypeToGravitinoType(Types.DecimalType.of(10, 2), DECIMAL, 10, 2);
    checkJdbcTypeToGravitinoType(Types.FixedCharType.of(20), CHAR, 20, null);
    checkJdbcTypeToGravitinoType(Types.VarCharType.of(20), VARCHAR, 20, null);
    checkJdbcTypeToGravitinoType(Types.StringType.get(), TEXT, null, null);
    checkJdbcTypeToGravitinoType(Types.BinaryType.get(), BLOB, null, null);
    checkJdbcTypeToGravitinoType(Types.BinaryType.get(), LONGBLOB, null, null);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withTimeZone(), TIMESTAMP,
        null,
        null);
    checkJdbcTypeToGravitinoType(Types.TimeType.get(), TIME, null, null);
    checkJdbcTypeToGravitinoType(Types.DateType.get(), DATE, null, null);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(), DATETIME, null, null);
    checkJdbcTypeToGravitinoType(
        Types.ExternalType.of(USER_DEFINED_TYPE), USER_DEFINED_TYPE, null, null);
  }

  @Test
  public void testFromGravitinoType() {
    //    checkGravitinoTypeToJdbcType(TINYINT, Types.ByteType.get());
    //    checkGravitinoTypeToJdbcType(UNSIGNED_TINYINT, Types.ByteType.unsigned());
    checkGravitinoTypeToJdbcType(TINYINT, Types.ByteType.get());
    checkGravitinoTypeToJdbcType(SMALLINT, Types.ShortType.get());
    checkGravitinoTypeToJdbcType(INT, Types.IntegerType.get());
    checkGravitinoTypeToJdbcType(BIGINT, Types.LongType.get());
    checkGravitinoTypeToJdbcType(FLOAT, Types.FloatType.get());
    checkGravitinoTypeToJdbcType(DOUBLE, Types.DoubleType.get());
    checkGravitinoTypeToJdbcType(TIME, Types.TimeType.get());
    checkGravitinoTypeToJdbcType(DATE, Types.DateType.get());
    checkGravitinoTypeToJdbcType(TIMESTAMP, Types.TimestampType.withTimeZone());
    checkGravitinoTypeToJdbcType(DATETIME, Types.TimestampType.withoutTimeZone());
    checkGravitinoTypeToJdbcType(DECIMAL + "(10,2)", Types.DecimalType.of(10, 2));
    checkGravitinoTypeToJdbcType(VARCHAR + "(20)", Types.VarCharType.of(20));
    checkGravitinoTypeToJdbcType(CHAR + "(20)", Types.FixedCharType.of(20));
    checkGravitinoTypeToJdbcType(LONGBLOB, Types.BinaryType.get());
    checkGravitinoTypeToJdbcType(USER_DEFINED_TYPE, Types.ExternalType.of(USER_DEFINED_TYPE));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GBASE_TYPE_CONVERTER.fromGravitino(Types.UnparsedType.of(USER_DEFINED_TYPE)));
  }

  protected void checkGravitinoTypeToJdbcType(String jdbcTypeName, Type gravitinoType) {
    Assertions.assertEquals(jdbcTypeName, GBASE_TYPE_CONVERTER.fromGravitino(gravitinoType));
  }

  protected void checkJdbcTypeToGravitinoType(
      Type gravitinoType, String jdbcTypeName, Integer columnSize, Integer scale) {
    JdbcTypeConverter.JdbcTypeBean typeBean = createTypeBean(jdbcTypeName, columnSize, scale);
    Assertions.assertEquals(gravitinoType, GBASE_TYPE_CONVERTER.toGravitino(typeBean));
  }

  protected static JdbcTypeConverter.JdbcTypeBean createTypeBean(
      String typeName, Integer columnSize, Integer scale) {
    return new JdbcTypeConverter.JdbcTypeBean(typeName) {
      {
        setColumnSize(columnSize);
        setScale(scale);
      }
    };
  }
}
