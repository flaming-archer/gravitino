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
package org.apache.gravitino.catalog.phoenix5.converter;

import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.BIGINT;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.BINARY;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.BOOLEAN;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.CHAR;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.DATE;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.DECIMAL;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.DOUBLE;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.FLOAT;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.INTEGER;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.SMALLINT;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.TIME;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.TIMESTAMP;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.TINYINT;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.UNSIGNED_DOUBLE;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.UNSIGNED_FLOAT;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.UNSIGNED_INT;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.UNSIGNED_LONG;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.UNSIGNED_SMALLINT;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.UNSIGNED_TIME;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.UNSIGNED_TIMESTAMP;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.UNSIGNED_TINYINT;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.VARBINARY;
import static org.apache.gravitino.catalog.phoenix5.converter.PhoenixTypeConverter.VARCHAR;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link PhoenixTypeConverter} */
public class TestPhoenixTypeConverter {

  private static final PhoenixTypeConverter PHOENIX_TYPE_CONVERTER =
      new PhoenixTypeConverter();
  private static final String USER_DEFINED_TYPE = "user-defined";

  @Test
  public void testToGravitinoType() {
    checkJdbcTypeToGravitinoType(Types.ByteType.get(), TINYINT, null, null);
    checkJdbcTypeToGravitinoType(Types.ByteType.unsigned(), UNSIGNED_TINYINT, null, null);
    checkJdbcTypeToGravitinoType(Types.ShortType.get(), SMALLINT, null, null);
    checkJdbcTypeToGravitinoType(Types.ShortType.unsigned(), UNSIGNED_SMALLINT, null, null);
    checkJdbcTypeToGravitinoType(Types.IntegerType.get(), INTEGER, null, null);
    checkJdbcTypeToGravitinoType(Types.IntegerType.unsigned(), UNSIGNED_INT, null, null);
    checkJdbcTypeToGravitinoType(Types.LongType.get(), BIGINT, null, null);
    checkJdbcTypeToGravitinoType(Types.LongType.unsigned(), UNSIGNED_LONG, null, null);
    checkJdbcTypeToGravitinoType(Types.FloatType.get(), FLOAT, null, null);
    checkJdbcTypeToGravitinoType(Types.FloatType.unsigned(), UNSIGNED_FLOAT, null, null);
    checkJdbcTypeToGravitinoType(Types.DoubleType.get(), DOUBLE, null, null);
    checkJdbcTypeToGravitinoType(Types.DoubleType.unsigned(), UNSIGNED_DOUBLE, null, null);
    checkJdbcTypeToGravitinoType(Types.TimeType.get(), TIME, null, null);
    checkJdbcTypeToGravitinoType(Types.TimeType.get(), UNSIGNED_TIME, null, null);
    checkJdbcTypeToGravitinoType(Types.DateType.get(), DATE, null, null);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withTimeZone(), TIMESTAMP, null, null);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(), UNSIGNED_TIMESTAMP, null, null);
    checkJdbcTypeToGravitinoType(Types.DecimalType.of(10, 2), DECIMAL, 10, 2);
    checkJdbcTypeToGravitinoType(Types.StringType.get(), VARCHAR, 20, null);
    checkJdbcTypeToGravitinoType(Types.FixedCharType.of(20), CHAR, 20, null);
    checkJdbcTypeToGravitinoType(Types.BooleanType.get(), BOOLEAN, 20, null);
    checkJdbcTypeToGravitinoType(Types.BinaryType.get(), BINARY, 20, null);
    checkJdbcTypeToGravitinoType(Types.BinaryType.get(), VARBINARY, 20, null);

    checkJdbcTypeToGravitinoType(
        Types.ExternalType.of(USER_DEFINED_TYPE), USER_DEFINED_TYPE, null, null);
  }

  @Test
  public void testFromGravitinoType() {
    checkGravitinoTypeToJdbcType(TINYINT, Types.ByteType.get());
    checkGravitinoTypeToJdbcType(UNSIGNED_TINYINT, Types.ByteType.unsigned());
    checkGravitinoTypeToJdbcType(SMALLINT, Types.ShortType.get());
    checkGravitinoTypeToJdbcType(UNSIGNED_SMALLINT, Types.ShortType.unsigned());
    checkGravitinoTypeToJdbcType(INTEGER, Types.IntegerType.get());
    checkGravitinoTypeToJdbcType(UNSIGNED_INT, Types.IntegerType.unsigned());
    checkGravitinoTypeToJdbcType(BIGINT, Types.LongType.get());
    checkGravitinoTypeToJdbcType(UNSIGNED_LONG, Types.LongType.unsigned());
    checkGravitinoTypeToJdbcType(FLOAT, Types.FloatType.get());
    checkGravitinoTypeToJdbcType(UNSIGNED_FLOAT, Types.FloatType.unsigned());
    checkGravitinoTypeToJdbcType(DOUBLE, Types.DoubleType.get());
    checkGravitinoTypeToJdbcType(UNSIGNED_DOUBLE, Types.DoubleType.unsigned());
    checkGravitinoTypeToJdbcType(TIME, Types.TimeType.get());
//    checkGravitinoTypeToJdbcType(UNSIGNED_TIME, Types.TimeType.get());

    checkGravitinoTypeToJdbcType(DATE, Types.DateType.get());
    checkGravitinoTypeToJdbcType(TIMESTAMP, Types.TimestampType.withTimeZone());
    checkGravitinoTypeToJdbcType(UNSIGNED_TIMESTAMP, Types.TimestampType.withoutTimeZone());
    checkGravitinoTypeToJdbcType(DECIMAL + "(10,2)", Types.DecimalType.of(10, 2));
    checkGravitinoTypeToJdbcType(VARCHAR, Types.VarCharType.of(20));
    checkGravitinoTypeToJdbcType(CHAR + "(20)", Types.FixedCharType.of(20));
    checkGravitinoTypeToJdbcType(BOOLEAN, Types.BooleanType.get());
    checkGravitinoTypeToJdbcType(BINARY, Types.BinaryType.get());
    checkGravitinoTypeToJdbcType(USER_DEFINED_TYPE, Types.ExternalType.of(USER_DEFINED_TYPE));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PHOENIX_TYPE_CONVERTER.fromGravitino(Types.UnparsedType.of(USER_DEFINED_TYPE)));
  }

  protected void checkGravitinoTypeToJdbcType(String jdbcTypeName, Type gravitinoType) {
    Assertions.assertEquals(jdbcTypeName, PHOENIX_TYPE_CONVERTER.fromGravitino(gravitinoType));
  }

  protected void checkJdbcTypeToGravitinoType(
      Type gravitinoType, String jdbcTypeName, Integer columnSize, Integer scale) {
    JdbcTypeConverter.JdbcTypeBean typeBean = createTypeBean(jdbcTypeName, columnSize, scale);
    Assertions.assertEquals(gravitinoType, PHOENIX_TYPE_CONVERTER.toGravitino(typeBean));
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
