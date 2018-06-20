/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package cn.gbase.flume.sink;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.source.http.HTTPSourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A Flume Sink that can publish messages to GBase 8a MPP cluster.
 * 
 * @author He Jiang
 */
public class GBase8aSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(GBase8aSink.class);

  private PassiveHttpSink httpSink;
  private String connectUrl;
  private String driverClassName;
  private String userName;
  private String passWord;
  private String loadSql;
  private int loadInterval = 0;
  private Connection conn = null;
  private Statement stm = null;

  private CounterGroup counterGroup;

  public GBase8aSink() {
    counterGroup = new CounterGroup();
    httpSink = new PassiveHttpSink();
  }

  @Override
  public void configure(Context context) {
    httpSink.configure(context);

    connectUrl = context.getString(GBase8aSinkConstants.CONNECTION_STRING);
    Preconditions.checkArgument(connectUrl != null && connectUrl.trim().length() != 0,
        "connect url must be a string");

    userName = context.getString(GBase8aSinkConstants.CONNECTION_USERNAME);
    passWord = context.getString(GBase8aSinkConstants.CONNECTION_PASSWORD);
    driverClassName = context.getString(GBase8aSinkConstants.CONNECTION_DRIVER_CLASS,
        GBase8aSinkConstants.DFLT_DRIVER_CLASS);
    
    loadInterval = context.getInteger(GBase8aSinkConstants.LOAD_INTERVAL,
        GBase8aSinkConstants.DFLT_LOAD_INTERVAL);
    Preconditions.checkArgument(loadInterval >= 0 && loadInterval < 30,
        "loadInterval must be in [0,30).");
    
    loadSql = context.getString(GBase8aSinkConstants.SQL_STRING);
    Preconditions.checkArgument(loadSql != null && loadSql.trim().length() != 0,
        "load sql must be a string");
    try {
      Integer port = context.getInteger(HTTPSourceConfigurationConstants.CONFIG_PORT);
      /* 获取本机hostname对应的IP */
      String IP = InetAddress.getLocalHost().getHostAddress();
      logger.info("local ip : {}, port : {}", IP, port);
      loadSql = loadSql.replaceAll("\\$\\{localhost\\}", IP + ":" + port);
    } catch (UnknownHostException e) {
      logger.error("Error while get local IP. Exception follows.", e);
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    
    try {
      stm.execute(loadSql);
      if (stm.getUpdateCount() == 0) {
        Thread.sleep(loadInterval * 1000);
      }
    } catch (Exception e) {
      logger.error("Error while execute  GBase8a. Exception follows.", e);
      reconnectGBase8a();
      status = Status.BACKOFF;
    }

    return status;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "GBase8aSink [httpSink=" + httpSink + ", connectUrl=" + connectUrl + ", driverClassName="
        + driverClassName + ", userName=" + userName + ", passWord=" + passWord + ", loadSql="
        + loadSql + ", conn=" + conn + ", stm=" + stm + ", counterGroup=" + counterGroup + "]";
  }

  /**
   * connect to GBase 8a MPP cluster.
   * 
   * @author chensj
   */
  private void connectGBase8a() {
    try {
      Class.forName(driverClassName);
      conn = DriverManager.getConnection(connectUrl, userName, passWord);
      stm = conn.createStatement();
      logger.info("connected GBase8a.");
    } catch (ClassNotFoundException e) {
      logger.error("Error while Connecting GBase8a. Exception follows.", e);
    } catch (SQLException e) {
      logger.error("Error while getConnection GBase8a. Exception follows.", e);
    }
  }

  private void reconnectGBase8a() {
    disconnectGBase8a();
    connectGBase8a();
  }

  @Override
  public void start() {
    logger.info("Starting {}...", this);

    counterGroup.setName(this.getName());
    connectGBase8a();
    httpSink.start();
    super.start();
    logger.info("GBase 8a sink {} started.", getName());
  }

  @Override
  public void stop() {
    logger.info("GBase 8a sink {} stopping...", getName());
    disconnectGBase8a();

    httpSink.stop();
    super.stop();

    logger.info("GBase 8a sink {} stopped. Event metrics: {}", getName(), counterGroup);
  }

  /**
   * 
   */
  private void disconnectGBase8a() {
    if (stm != null) {
      try {
        if (!stm.isClosed()) {
          stm.close();
        }
      } catch (Exception e) {
        logger.error("Error while close stm. Exception follows.", e);
      }
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          logger.error("Error while close connect. Exception follows.", e);
        }
      }
    }

  }

  @Override
  public synchronized void setChannel(Channel channel) {
    httpSink.setChannel(channel);
    super.setChannel(channel);
  }
}
