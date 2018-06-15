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

package org.apache.flume.sink.gbase;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceConfigurationConstants;
import org.apache.flume.tools.FlumeBeanConfigurator;
import org.apache.flume.tools.HTTPServerConstraintUtil;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A Flume Sink that waits for clients to pull events
 * 
 * @author He Jiang
 */
public class PassiveHttpSink extends AbstractSink implements Configurable {
  /**
   * There are 2 ways of doing this: 
   * a. Have a static server instance and use connectors in each source which binds to the port
   *    defined for that source. 
   * b. Each source starts its own server instance, which binds to the source's port.
   *
   * b is more efficient than a because Jetty does not allow binding a servlet to a connector. So
   * each request will need to go through each each of the handlers/servlet till the correct one is
   * found.
   *
   */

  private static final Logger LOG = LoggerFactory.getLogger(PassiveHttpSink.class);
  private volatile Integer port;
  private volatile Server srv;
  private volatile String host;
  private PassiveHttpSinkHandler handler;
  private SinkCounter sinkCounter;

  // SSL configuration variable
  private volatile String keyStorePath;
  private volatile String keyStorePassword;
  private volatile Boolean sslEnabled;
  private final List<String> excludedProtocols = new LinkedList<String>();

  private Context sinkContext;

  @Override
  public void configure(Context context) {
    sinkContext = context;
    try {
      // SSL related config
      sslEnabled = context.getBoolean(HTTPSourceConfigurationConstants.SSL_ENABLED, false);

      port = context.getInteger(HTTPSourceConfigurationConstants.CONFIG_PORT);
      host = context.getString(HTTPSourceConfigurationConstants.CONFIG_BIND,
          HTTPSourceConfigurationConstants.DEFAULT_BIND);

      Preconditions.checkState(host != null && !host.isEmpty(),
          "PassiveHttpSink hostname specified is empty");
      Preconditions.checkNotNull(port,
          "PassiveHttpSink requires a port number to be" + " specified");

      String handlerClassName = context.getString(HTTPSourceConfigurationConstants.CONFIG_HANDLER,
          GBase8aSinkConstants.DFLT_HANDLER).trim();

      if (sslEnabled) {
        LOG.debug("SSL configuration enabled");
        keyStorePath = context.getString(HTTPSourceConfigurationConstants.SSL_KEYSTORE);
        Preconditions.checkArgument(keyStorePath != null && !keyStorePath.isEmpty(),
            "Keystore is required for SSL Conifguration");
        keyStorePassword = context
            .getString(HTTPSourceConfigurationConstants.SSL_KEYSTORE_PASSWORD);
        Preconditions.checkArgument(keyStorePassword != null,
            "Keystore password is required for SSL Configuration");
        String excludeProtocolsStr = context
            .getString(HTTPSourceConfigurationConstants.EXCLUDE_PROTOCOLS);
        if (excludeProtocolsStr == null) {
          excludedProtocols.add("SSLv3");
        } else {
          excludedProtocols.addAll(Arrays.asList(excludeProtocolsStr.split(" ")));
          if (!excludedProtocols.contains("SSLv3")) {
            excludedProtocols.add("SSLv3");
          }
        }
      }

      @SuppressWarnings("unchecked")
      Class<? extends PassiveHttpSinkHandler> clazz = (Class<? extends PassiveHttpSinkHandler>) Class
          .forName(handlerClassName);
      handler = clazz.getDeclaredConstructor().newInstance();
      handler.setSink(this);

      Map<String, String> subProps = context
          .getSubProperties(HTTPSourceConfigurationConstants.CONFIG_HANDLER_PREFIX);
      handler.configure(new Context(subProps));
    } catch (ClassNotFoundException ex) {
      LOG.error("Error while configuring PassiveHttpSink. Exception follows.", ex);
      Throwables.propagate(ex);
    } catch (ClassCastException ex) {
      LOG.error("Deserializer is not an instance of PassiveHttpSinkHandler."
          + "Deserializer must implement PassiveHttpSinkHandler.");
      Throwables.propagate(ex);
    } catch (Exception ex) {
      LOG.error("Error configuring PassiveHttpSink!", ex);
      Throwables.propagate(ex);
    }
    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  @Override
  public void start() {
    Preconditions.checkState(srv == null, "Running HTTP Server found in source: " + getName()
        + " before I started one." + "Will not attempt to start.");
    QueuedThreadPool threadPool = new QueuedThreadPool();
    if (sinkContext.getSubProperties("QueuedThreadPool.").size() > 0) {
      FlumeBeanConfigurator.setConfigurationFields(threadPool, sinkContext);
    }
    srv = new Server(threadPool);

    // Register with JMX for advanced monitoring
    MBeanContainer mbContainer = new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
    srv.addEventListener(mbContainer);
    srv.addBean(mbContainer);

    HttpConfiguration httpConfiguration = new HttpConfiguration();
    httpConfiguration.addCustomizer(new SecureRequestCustomizer());

    FlumeBeanConfigurator.setConfigurationFields(httpConfiguration, sinkContext);
    ServerConnector connector;

    if (sslEnabled) {
      SslContextFactory sslCtxFactory = new SslContextFactory();
      FlumeBeanConfigurator.setConfigurationFields(sslCtxFactory, sinkContext);
      sslCtxFactory.setExcludeProtocols(excludedProtocols.toArray(new String[0]));
      sslCtxFactory.setKeyStorePath(keyStorePath);
      sslCtxFactory.setKeyStorePassword(keyStorePassword);

      httpConfiguration.setSecurePort(port);
      httpConfiguration.setSecureScheme("https");

      connector = new ServerConnector(srv,
          new SslConnectionFactory(sslCtxFactory, HttpVersion.HTTP_1_1.asString()),
          new HttpConnectionFactory(httpConfiguration));
    } else {
      connector = new ServerConnector(srv, new HttpConnectionFactory(httpConfiguration));
    }

    connector.setPort(port);
    connector.setHost(host);
    connector.setReuseAddress(true);

    FlumeBeanConfigurator.setConfigurationFields(connector, sinkContext);

    srv.addConnector(connector);

    try {
      ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
      context.setContextPath("/");
      srv.setHandler(context);

      context.addServlet(new ServletHolder(new FlumeHTTPServlet()), "/");
      context.setSecurityHandler(HTTPServerConstraintUtil.enforceConstraints());
      srv.start();
    } catch (Exception ex) {
      LOG.error("Error while starting PassiveHttpSink. Exception follows.", ex);
      Throwables.propagate(ex);
    }
    Preconditions.checkArgument(srv.isRunning());
    sinkCounter.start();
    super.start();
  }

  @Override
  public void stop() {
    try {
      srv.stop();
      srv.join();
      srv = null;
    } catch (Exception ex) {
      LOG.error("Error while stopping PassiveHttpSink. Exception follows.", ex);
    }
    sinkCounter.stop();
    LOG.info("PassiveHttpSink {} stopped. Metrics: {}", getName(), sinkCounter);
  }

  private class FlumeHTTPServlet extends HttpServlet {

    private static final long serialVersionUID = 4891924853218790342L;

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      sinkCounter.incrementEventDrainAttemptCount();
      long handledEventSize = 0;
      try {
        handledEventSize = handler.handle(request, response);
      } catch (HTTPBadRequestException ex) {
        LOG.warn("Received bad request from client. ", ex);
        response.sendError(HttpServletResponse.SC_BAD_REQUEST,
            "Bad request from client. " + ex.getMessage());
        return;
      } catch (Exception ex) {
        LOG.warn("Unexpected error while sending events. ", ex);
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            "Unexpected error while sending events. " + ex.getMessage());
        return;
      }

      response.flushBuffer();

      if (handledEventSize == 0) {
        sinkCounter.incrementBatchEmptyCount();
      }
      sinkCounter.incrementBatchCompleteCount();
      sinkCounter.addToEventDrainSuccessCount(handledEventSize);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
      doPost(request, response);
    }

    @Override
    protected void doHead(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      try {
        handler.handle(request, response);
      } catch (HTTPBadRequestException ex) {
        LOG.warn("Received bad request from client. ", ex);
        response.sendError(HttpServletResponse.SC_BAD_REQUEST,
            "Bad request from client. " + ex.getMessage());
        return;
      } catch (Exception ex) {
        LOG.warn("Unexpected error while sending events. ", ex);
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            "Unexpected error while sending events. " + ex.getMessage());
        return;
      }

      response.flushBuffer();
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    // do nothing
    return Status.BACKOFF;
  }

}