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
package org.apache.gravitino.integration.test.container;

import static java.lang.String.format;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PhoenixContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(PhoenixContainer.class);

  public static final String DEFAULT_IMAGE = "igoragbash/apache-phoenix:2.0.2-5.1.2";
  public static final String HOST_NAME = "gravitino-ci-phoenix";
  public static final int PHOENIX_PORT = 2181;
  public static final String USER_NAME = "root";
  public static final String PASSWORD = "root";

  public static Builder builder() {
    return new Builder();
  }

  protected PhoenixContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
  }

  @Override
  protected void setupContainer() {
    super.setupContainer();
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "phoenixContainer")));
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("phoenix container startup failed!", checkContainerStatus(5));
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    return true;
  }

  public void createDatabase(TestDatabaseName testDatabaseName) {
    String phoenixJdbcUrl =
        StringUtils.substring(
            getJdbcUrl(testDatabaseName), 0, getJdbcUrl(testDatabaseName).lastIndexOf("/"));

    // change password for root user, Gravitino API must set password in catalog properties
    try (Connection connection =
            DriverManager.getConnection(phoenixJdbcUrl, USER_NAME, getPassword());
        Statement statement = connection.createStatement()) {

      String query = String.format("CREATE SCHEMA %s ", testDatabaseName);
      // FIXME: String, which is used in SQL, can be unsafe
      statement.execute(query);
      LOG.info(
          String.format("phoenix container database %s has been created", testDatabaseName));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public String getUsername() {
    return USER_NAME;
  }

  public String getPassword() {
    return PASSWORD;
  }

  public String getJdbcUrl() {
    return format("jdbc:phoenix:%s:%d", getContainerIpAddress(), PHOENIX_PORT);
  }

  public String getJdbcUrl(TestDatabaseName testDatabaseName) {
    return format(
        "jdbc:phoenix:%s:%d/%s", getContainerIpAddress(), PHOENIX_PORT, testDatabaseName);
  }

  public String getDriverClassName(TestDatabaseName testDatabaseName) throws SQLException {
    return DriverManager.getDriver(getJdbcUrl(testDatabaseName)).getClass().getName();
  }

  public static class Builder
      extends BaseContainer.Builder<PhoenixContainer.Builder, PhoenixContainer> {

    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(PHOENIX_PORT);
    }

    @Override
    public PhoenixContainer build() {
      return new PhoenixContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
