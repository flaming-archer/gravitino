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

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/**
 * Type converter for Gbase.
 */
public class GbaseTypeConverter extends JdbcTypeConverter {

  static final String TINYINT = "TINYINT";
  static final String SMALLINT = "SMALLINT";
  static final String INT = "INT";
  static final String BIGINT = "BIGINT";
  static final String FLOAT = "FLOAT";
  static final String DOUBLE = "DOUBLE";
  static final String DECIMAL = "DECIMAL";
  static final String NUMERIC = "NUMERIC";

  static final String CHAR = "CHAR";
  static final String VARCHAR = "VARCHAR";
  static final String TEXT = "TEXT";

  static final String BLOB = "BLOB";
  static final String LONGBLOB = "LONGBLOB";

  static final String DATE = "DATE";
  static final String DATETIME = "DATETIME";
  static final String TIME = "TIME";
  static final String TIMESTAMP = "TIMESTAMP";

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    String typeName = typeBean.getTypeName();

    switch (typeName) {
      case TINYINT:
        return Types.ByteType.get();
      case SMALLINT:
        return Types.ShortType.get();
      case INT:
        return Types.IntegerType.get();
      case BIGINT:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case DECIMAL:
      case NUMERIC:
        return Types.DecimalType.of(typeBean.getColumnSize(), typeBean.getScale());
      case CHAR:
        return Types.FixedCharType.of(typeBean.getColumnSize());
      case VARCHAR:
        return Types.VarCharType.of(typeBean.getColumnSize());
      case TEXT:
        return Types.StringType.get();
      case DATE:
        return Types.DateType.get();
      case TIMESTAMP:
        return Types.TimestampType.withTimeZone();
      case TIME:
        return Types.TimeType.get();
      case DATETIME:
        return Types.TimestampType.withoutTimeZone();
      case BLOB:
      case LONGBLOB:
        return Types.BinaryType.get();
      default:
        return Types.ExternalType.of(typeBean.getTypeName());
    }
  }

  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.ByteType) {
      return TINYINT;
    } else if (type instanceof Types.ShortType) {
      return SMALLINT;
    } else if (type instanceof Types.IntegerType) {
      return INT;
    } else if (type instanceof Types.LongType) {
      return BIGINT;
    } else if (type instanceof Types.FloatType) {
      return FLOAT;
    } else if (type instanceof Types.DoubleType) {
      return DOUBLE;
    } else if (type instanceof Types.StringType) {
      return VARCHAR;
    } else if (type instanceof Types.DateType) {
      return DATE;
    } else if (type instanceof Types.TimestampType) {
      return ((Types.TimestampType) type).hasTimeZone() ? TIMESTAMP : DATETIME;
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
      return VARCHAR + "(" + ((Types.VarCharType) type).length() + ")";
    } else if (type instanceof Types.FixedCharType) {
      return CHAR + "(" + ((Types.FixedCharType) type).length() + ")";
    } else if (type instanceof Types.BinaryType) {
      return LONGBLOB;
    } else if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    throw new IllegalArgumentException(
        String.format(
            "Couldn't convert Gravitino type %s to Gbase type", type.simpleString()));
  }
}
