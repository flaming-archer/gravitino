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

import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;

import java.sql.SQLException;

/**
 * Exception converter to Apache Gravitino exception for Phoenix.
 */
public class PhoenixExceptionConverter extends JdbcExceptionConverter {

  static final int DATABASE_ALREADY_EXISTS = 721;

  static final int TABLE_ALREADY_EXISTS = 1013;
  static final int TABLE_NOT_IN_REGION = 1145;
  static final int TABLE_UNDEFINED = 1012;
  static final int DATABASE_NOT_FOUND = 722;

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException sqlException) {
    int errorCode = sqlException.getErrorCode();
    switch (errorCode) {
      case DATABASE_NOT_FOUND:
        return new NoSuchSchemaException(sqlException, sqlException.getMessage());
      case DATABASE_ALREADY_EXISTS:
        return new SchemaAlreadyExistsException(sqlException, sqlException.getMessage());
      case TABLE_ALREADY_EXISTS:
        return new TableAlreadyExistsException(sqlException, sqlException.getMessage());

      case TABLE_NOT_IN_REGION:
      case TABLE_UNDEFINED:
        return new NoSuchTableException(sqlException, sqlException.getMessage());
      default:
        return new GravitinoRuntimeException(sqlException, sqlException.getMessage());
    }

  }
}
