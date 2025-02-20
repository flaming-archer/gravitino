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

import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tag("gravitino-docker-test")
public class TestPhoenixDatabaseOperations extends TestPhoenix {

  @Test
  public void testBaseOperationDatabase() {
    //phoenix database is uppercase
    String databaseName = RandomNameUtils.genRandomName("ct_db").toUpperCase();
    Map<String, String> properties = new HashMap<>();
    String comment = null;
    List<String> databases = DATABASE_OPERATIONS.listDatabases();
    ((PhoenixDatabaseOperations) DATABASE_OPERATIONS)
        .createSysDatabaseNameSet()
        .forEach(
            phoneixDatabaseName ->
                Assertions.assertFalse(databases.contains(phoneixDatabaseName)));
    testBaseOperation(databaseName, properties, comment);

    testDropDatabase(databaseName);
  }
}
