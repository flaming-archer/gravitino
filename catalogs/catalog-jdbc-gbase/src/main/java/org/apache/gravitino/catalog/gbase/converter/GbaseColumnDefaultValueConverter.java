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
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.DATETIME;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.DECIMAL;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.DOUBLE;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.FLOAT;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.INT;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.LONGBLOB;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.SMALLINT;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.TEXT;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.TINYINT;
import static org.apache.gravitino.catalog.gbase.converter.GbaseTypeConverter.VARCHAR;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class GbaseColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {


  public String fromGravitino(Expression defaultValue) {
    if (DEFAULT_VALUE_NOT_SET.equals(defaultValue)) {
      return null;
    }

    if (defaultValue instanceof FunctionExpression) {
      FunctionExpression functionExpression = (FunctionExpression) defaultValue;
      return String.format("(%s)", functionExpression);
    }

    if (defaultValue instanceof Literal) {
      Literal<?> literal = (Literal<?>) defaultValue;
      Type type = literal.dataType();
      if (defaultValue.equals(Literals.NULL)) {
        return NULL;
      } else if (type instanceof Type.NumericType) {
        return literal.value().toString();
      } else {
        Object value = literal.value();
        if (value instanceof LocalDateTime) {
          value = ((LocalDateTime) value).format(DATE_TIME_FORMATTER);
        }
        return String.format("'%s'", value);
      }
    }

    throw new IllegalArgumentException("Not a supported column default value: " + defaultValue);
  }

  @Override
  public Expression toGravitino(JdbcTypeConverter.JdbcTypeBean type, String columnDefaultValue,
      boolean isExpression, boolean nullable) {
    //Gbase don't support col expression
    if (columnDefaultValue == null || columnDefaultValue.isEmpty()) {
      return nullable ? Literals.NULL : DEFAULT_VALUE_NOT_SET;
    }

    String reallyType = type.getTypeName();

    if (nullable) {
      if (columnDefaultValue.equals("NULL")) {
        return Literals.NULL;
      }
    }

    // need exclude begin and end "'"
    String reallyValue = columnDefaultValue.startsWith("'") ? columnDefaultValue.substring(1,
        columnDefaultValue.length() - 1) : columnDefaultValue;

    try {
      switch (reallyType) {
        case TINYINT:
          return Literals.byteLiteral(Byte.valueOf(reallyValue));
        case SMALLINT:
          return Literals.shortLiteral(Short.valueOf(reallyValue));
        case INT:
          return Literals.integerLiteral(Integer.valueOf(reallyValue));
        case BIGINT:
          return Literals.longLiteral(Long.valueOf(reallyValue));
        case FLOAT:
          return Literals.floatLiteral(Float.valueOf(reallyValue));
        case DOUBLE:
          return Literals.doubleLiteral(Double.valueOf(reallyValue));
        case DECIMAL:
          if (reallyValue.equals("0.")) {
            reallyValue = "0.0";
          }
          return Literals.decimalLiteral(
              Decimal.of(reallyValue, type.getColumnSize(), type.getScale()));
        case CHAR:
          return Literals.of(reallyValue, Types.FixedCharType.of(type.getColumnSize()));
        case VARCHAR:
          return Literals.of(reallyValue, Types.StringType.get());
        case TEXT:
          return Literals.of(reallyValue, Types.StringType.get());
        case BLOB:
        case LONGBLOB:
          return Literals.of(reallyValue, Types.BinaryType.get());
        case JdbcTypeConverter.DATE:
          return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_TIME_FORMATTER));
        case JdbcTypeConverter.TIME:
          return Literals.timeLiteral(LocalTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
        case JdbcTypeConverter.TIMESTAMP:
        case DATETIME:
          return CURRENT_TIMESTAMP.equals(columnDefaultValue)
              ? DEFAULT_VALUE_OF_CURRENT_TIMESTAMP
              : Literals.timestampLiteral(
                  LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
        default:
          return UnparsedExpression.of(reallyValue);
      }
    } catch (Exception ex) {
      return UnparsedExpression.of(reallyValue);
    }
  }
}
