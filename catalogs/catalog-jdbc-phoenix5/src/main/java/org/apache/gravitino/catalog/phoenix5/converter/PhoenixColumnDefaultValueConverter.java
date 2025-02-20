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

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

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

public class PhoenixColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {


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
  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean type,
      String columnDefaultValue,
      boolean isExpression,
      boolean nullable) {
    //Phoenix don't support col expression
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
    String reallyValue =
        columnDefaultValue.startsWith("'")
            ? columnDefaultValue.substring(1, columnDefaultValue.length() - 1)
            : columnDefaultValue;

    try {
      switch (reallyType) {
        case PhoenixTypeConverter.TINYINT:
          return Literals.byteLiteral(Byte.valueOf(reallyValue));
        case PhoenixTypeConverter.UNSIGNED_TINYINT:
          return Literals.unsignedByteLiteral(Short.valueOf(reallyValue));
        case PhoenixTypeConverter.SMALLINT:
          return Literals.shortLiteral(Short.valueOf(reallyValue));
        case PhoenixTypeConverter.UNSIGNED_SMALLINT:
          return Literals.unsignedShortLiteral(Integer.valueOf(reallyValue));
        case PhoenixTypeConverter.INTEGER:
          return Literals.integerLiteral(Integer.valueOf(reallyValue));
        case PhoenixTypeConverter.UNSIGNED_INT:
          return Literals.unsignedIntegerLiteral(Long.valueOf(reallyValue));
        case PhoenixTypeConverter.BIGINT:
          return Literals.longLiteral(Long.valueOf(reallyValue));
        case PhoenixTypeConverter.UNSIGNED_LONG:
          return Literals.unsignedLongLiteral(Decimal.of(reallyValue));
        case PhoenixTypeConverter.FLOAT:
          return Literals.floatLiteral(Float.valueOf(reallyValue));
        case PhoenixTypeConverter.UNSIGNED_FLOAT:
          return Literals.floatLiteral(Float.valueOf(reallyValue));
        case PhoenixTypeConverter.DOUBLE:
          return Literals.doubleLiteral(Double.valueOf(reallyValue));
        case PhoenixTypeConverter.UNSIGNED_DOUBLE:
          return Literals.doubleLiteral(Double.valueOf(reallyValue));
        case PhoenixTypeConverter.DECIMAL:
          if (reallyValue.equals("0.")) {
            reallyValue = "0.0";
          }
          return Literals.decimalLiteral(
              Decimal.of(reallyValue, type.getColumnSize(), type.getScale()));
        case PhoenixTypeConverter.DATE:
          if (reallyValue.equals("")) {
            return Literals.NULL;
          }
          return Literals.dateLiteral(LocalDate.parse(reallyValue, DATE_FORMATTER));
        case PhoenixTypeConverter.TIME:
        case PhoenixTypeConverter.UNSIGNED_TIME:
          return Literals.timeLiteral(
              LocalTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
        case PhoenixTypeConverter.TIMESTAMP:
        case PhoenixTypeConverter.UNSIGNED_TIMESTAMP:
          return CURRENT_TIMESTAMP.equals(reallyValue)
              ? DEFAULT_VALUE_OF_CURRENT_TIMESTAMP
              : Literals.timestampLiteral(LocalDateTime.parse(reallyValue, DATE_TIME_FORMATTER));
        case PhoenixTypeConverter.CHAR:
          return Literals.of(reallyValue, Types.FixedCharType.of(type.getColumnSize()));
        case PhoenixTypeConverter.VARCHAR:
          return Literals.of(reallyValue, Types.StringType.get());
        case PhoenixTypeConverter.BINARY:
        case PhoenixTypeConverter.VARBINARY:
          return Literals.of(reallyValue, Types.BinaryType.get());
        default:
          return UnparsedExpression.of(reallyValue);
      }
    } catch (Exception ex) {
      return UnparsedExpression.of(reallyValue);
    }
  }
}
