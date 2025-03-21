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

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/**
 * Type converter for Phoenix.
 */
public class PhoenixTypeConverter extends JdbcTypeConverter {

  static final String INTEGER = "INTEGER";
  static final String UNSIGNED_INT = "UNSIGNED_INT";
  static final String BIGINT = "BIGINT";
  static final String UNSIGNED_LONG = "UNSIGNED_LONG";
  static final String TINYINT = "TINYINT";
  static final String UNSIGNED_TINYINT = "UNSIGNED_TINYINT";
  static final String SMALLINT = "SMALLINT";
  static final String UNSIGNED_SMALLINT = "UNSIGNED_SMALLINT";
  static final String DECIMAL = "DECIMAL";
  static final String FLOAT = "FLOAT";
  static final String UNSIGNED_FLOAT = "UNSIGNED_FLOAT";
  static final String UNSIGNED_DOUBLE = "UNSIGNED_DOUBLE";
  static final String DOUBLE = "DOUBLE";
  static final String BOOLEAN = "BOOLEAN";
  static final String TIME = "TIME";
  static final String DATE = "DATE";
  static final String TIMESTAMP = "TIMESTAMP";
  static final String UNSIGNED_TIME = "UNSIGNED_TIME";
  static final String UNSIGNED_DATE = "UNSIGNED_DATE";
  static final String UNSIGNED_TIMESTAMP = "UNSIGNED_TIMESTAMP";
  static final String CHAR = "CHAR";
  static final String VARCHAR = "VARCHAR";
  static final String BINARY = "BINARY";
  static final String VARBINARY = "VARBINARY";
  static final String ARRAY = "ARRAY";

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    String typeName = typeBean.getTypeName();

    switch (typeName) {
      case INTEGER:
        return Types.IntegerType.get();
      case UNSIGNED_INT:
        return Types.IntegerType.unsigned();
      case BIGINT:
        return Types.LongType.get();
      case UNSIGNED_LONG:
        return Types.LongType.unsigned();
      case TINYINT:
        return Types.ByteType.get();
      case UNSIGNED_TINYINT:
        return Types.ByteType.unsigned();
      case SMALLINT:
        return Types.ShortType.get();
      case UNSIGNED_SMALLINT:
        return Types.ShortType.unsigned();
      case DECIMAL:
        return Types.DecimalType.of(typeBean.getColumnSize(), typeBean.getScale());
      case FLOAT:
        return Types.FloatType.get();
      case UNSIGNED_FLOAT:
        return Types.FloatType.unsigned();
      case UNSIGNED_DOUBLE:
        return Types.DoubleType.unsigned();
      case DOUBLE:
        return Types.DoubleType.get();
      case BOOLEAN:
        return Types.BooleanType.get();
      case TIME:
        return Types.TimeType.get();
      case DATE:
        return Types.DateType.get();
      case TIMESTAMP:
        return Types.TimestampType.withTimeZone();
      case UNSIGNED_TIME:
        return Types.TimeType.get();
      case UNSIGNED_DATE:
        return Types.DateType.get();
      case UNSIGNED_TIMESTAMP:
        return Types.TimestampType.withoutTimeZone();
      case CHAR:
        return Types.FixedCharType.of(typeBean.getColumnSize());
      case VARCHAR:
        return Types.StringType.get();
      case BINARY:
        return Types.BinaryType.get();
      case VARBINARY:
        return Types.BinaryType.get();
      case ARRAY:
        //TODO maybe can implements?
        return Types.ExternalType.of(typeBean.getTypeName());
      default:
        return Types.ExternalType.of(typeBean.getTypeName());
    }
  }

  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.ByteType) {
      if (((Types.ByteType) type).signed()) {
        return TINYINT;
      } else {
        return UNSIGNED_TINYINT;
      }
    } else if (type instanceof Types.ShortType) {
      if (((Types.ShortType) type).signed()) {
        return SMALLINT;
      } else {
        return UNSIGNED_SMALLINT;
      }
    } else if (type instanceof Types.IntegerType) {
      if (((Types.IntegerType) type).signed()) {
        return INTEGER;
      } else {
        return UNSIGNED_INT;
      }
    } else if (type instanceof Types.LongType) {
      if (((Types.LongType) type).signed()) {
        return BIGINT;
      } else {
        return UNSIGNED_LONG;
      }
    } else if (type instanceof Types.FloatType) {
      if (((Types.FloatType) type).signed()) {
        return FLOAT;
      } else {
        return UNSIGNED_FLOAT;
      }
    } else if (type instanceof Types.DoubleType) {
      if (((Types.DoubleType) type).signed()) {
        return DOUBLE;
      } else {
        return UNSIGNED_DOUBLE;
      }
    } else if (type instanceof Types.StringType) {
      return VARCHAR;
    } else if (type instanceof Types.DateType) {
      return DATE;
    } else if (type instanceof Types.TimestampType) {
      return ((Types.TimestampType) type).hasTimeZone() ? TIMESTAMP : UNSIGNED_TIMESTAMP;
    } else if (type instanceof Types.TimeType) {
      return TIME;
    } else if (type instanceof Types.DecimalType) {
      return DECIMAL
          + "("
          + ((Types.DecimalType) type).precision()
          + ","
          + ((Types.DecimalType) type).scale()
          + ")";
    } else if (type instanceof Types.VarCharType) {
      return VARCHAR;
    } else if (type instanceof Types.FixedCharType) {
      return CHAR + "(" + ((Types.FixedCharType) type).length() + ")";
    } else if (type instanceof Types.BooleanType) {
      return BOOLEAN;
    } else if (type instanceof Types.BinaryType) {
      return BINARY;
    } else if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    throw new IllegalArgumentException(
        String.format(
            "Couldn't convert Gravitino type %s to Phoenix type", type.simpleString()));
  }
}
