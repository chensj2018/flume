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

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPSourceConfigurationConstants;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;

import cn.gbase.flume.sink.GBase8aSinkConstants;
import cn.gbase.flume.sink.PassiveHttpSink;
import junit.framework.Assert;

/**
 *
 */
public class TestPassiveHttpSink {

  private static PassiveHttpSink sink;
  private static PassiveHttpSink httpsSink;

  private static Channel channel;
  private static Channel httpsChannel;
  private static int selectedPort;
  private static int sslPort;
  HttpClient httpClient;
  HttpPost postRequest;

  private static int findFreePort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  private static Context getDefaultNonSecureContext(int selectedPort) throws IOException {
    Context ctx = new Context();
    ctx.put(HTTPSourceConfigurationConstants.CONFIG_BIND, "0.0.0.0");
    ctx.put(HTTPSourceConfigurationConstants.CONFIG_PORT, String.valueOf(selectedPort));
    ctx.put("QueuedThreadPool.MaxThreads", "100");

    return ctx;
  }

  private static Context getDefaultSecureContext(int sslPort) throws IOException {
    Context sslContext = new Context();
    sslContext.put(HTTPSourceConfigurationConstants.CONFIG_PORT, String.valueOf(sslPort));
    sslContext.put(HTTPSourceConfigurationConstants.SSL_ENABLED, "true");
    sslContext.put(HTTPSourceConfigurationConstants.SSL_KEYSTORE_PASSWORD, "password");
    sslContext.put(HTTPSourceConfigurationConstants.SSL_KEYSTORE, "src/test/resources/jettykeystore");

    sslContext.put(HTTPSourceConfigurationConstants.CONFIG_HANDLER_PREFIX + GBase8aSinkConstants.CONTENT_TYPE, "text/plain");
    sslContext.put(HTTPSourceConfigurationConstants.CONFIG_HANDLER_PREFIX + GBase8aSinkConstants.CHARACTER_ENCODING, "gbk");
    sslContext.put(HTTPSourceConfigurationConstants.CONFIG_HANDLER_PREFIX + GBase8aSinkConstants.BATCH_SIZE, "2");
    sslContext.put(HTTPSourceConfigurationConstants.CONFIG_HANDLER_PREFIX + GBase8aSinkConstants.CONTENT_PREFIX, "[");
    sslContext.put(HTTPSourceConfigurationConstants.CONFIG_HANDLER_PREFIX + GBase8aSinkConstants.CONTENT_SURFFIX, "]");
    sslContext.put(HTTPSourceConfigurationConstants.CONFIG_HANDLER_PREFIX + GBase8aSinkConstants.CONTENT_SEPARATOR, ", ");

    return sslContext;
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    sink = new PassiveHttpSink();
    channel = new MemoryChannel();
    selectedPort = findFreePort();
    configureSinkAndChannel(sink, channel, getDefaultNonSecureContext(selectedPort));
    channel.start();
    sink.setChannel(channel);
    sink.start();

    httpsSink = new PassiveHttpSink();
    httpsChannel = new MemoryChannel();
    sslPort = findFreePort();
    configureSinkAndChannel(httpsSink, httpsChannel, getDefaultSecureContext(sslPort));
    httpsChannel.start();
    httpsSink.setChannel(httpsChannel);
    httpsSink.start();
  }

  private static void configureSinkAndChannel(PassiveHttpSink sink, Channel channel, Context context) {
    Context channelContext = new Context();
    channelContext.put("capacity", "100");
    Configurables.configure(channel, channelContext);
    Configurables.configure(sink, context);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    sink.stop();
    channel.stop();
    httpsSink.stop();
    httpsChannel.stop();
  }

  @Before
  public void setUp() {
    HttpClientBuilder builder = HttpClientBuilder.create();
    httpClient = builder.build();
    postRequest = new HttpPost("http://0.0.0.0:" + selectedPort);
  }

  @Test
  public void testSimple() throws IOException, InterruptedException {
    StringBuilder expected = new StringBuilder();

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    for (int i = 0; i < 2; i++) {
      Event event = EventBuilder.withBody("test event " + (i + 1), Charsets.UTF_8);
      channel.put(event);
      expected.append(new String(event.getBody(), Charsets.UTF_8));
    }
    transaction.commit();
    transaction.close();

    HttpResponse response = httpClient.execute(postRequest);

    Assert.assertEquals(HttpServletResponse.SC_OK, response.getStatusLine().getStatusCode());

    HttpEntity entity = response.getEntity();

    Header header = entity.getContentType();
    Assert.assertEquals("application/json;charset=utf-8", header.getValue());

    header = response.getFirstHeader("Transfer-Encoding");
    Assert.assertEquals("chunked", header.getValue());

    InputStream stream = entity.getContent();
    String str = IOUtils.toString(stream);
    Assert.assertEquals(expected.toString(), str);

    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = channel.take();
    Assert.assertNull(e);
    tx.commit();
    tx.close();
  }

  @Test
  public void testTrace() throws Exception {
    doTestForbidden(new HttpTrace("http://0.0.0.0:" + selectedPort));
  }

  @Test
  public void testOptions() throws Exception {
    doTestForbidden(new HttpOptions("http://0.0.0.0:" + selectedPort));
  }

  private void doTestForbidden(HttpRequestBase request) throws Exception {
    HttpResponse response = httpClient.execute(request);
    Assert.assertEquals(HttpServletResponse.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testHttps() throws Exception {
    doTestHttps(null, sslPort);
  }

  @Test(expected = javax.net.ssl.SSLHandshakeException.class)
  public void testHttpsSSLv3() throws Exception {
    doTestHttps("SSLv3", sslPort);
  }

  public void doTestHttps(String protocol, int port) throws Exception {
    HttpsURLConnection httpsURLConnection = null;
    try {
      TrustManager[] trustAllCerts = { new X509TrustManager() {
        @Override
        public void checkClientTrusted(java.security.cert.X509Certificate[] x509Certificates, String s) throws CertificateException {
          // noop
        }

        @Override
        public void checkServerTrusted(java.security.cert.X509Certificate[] x509Certificates, String s) throws CertificateException {
          // noop
        }

        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return null;
        }
      } };

      SSLContext sc = null;
      javax.net.ssl.SSLSocketFactory factory = null;
      if (System.getProperty("java.vendor").contains("IBM")) {
        sc = SSLContext.getInstance("SSL_TLS");
      } else {
        sc = SSLContext.getInstance("SSL");
      }

      HostnameVerifier hv = new HostnameVerifier() {
        public boolean verify(String arg0, SSLSession arg1) {
          return true;
        }
      };
      sc.init(null, trustAllCerts, new SecureRandom());

      if (protocol != null) {
        factory = new DisabledProtocolsSocketFactory(sc.getSocketFactory(), protocol);
      } else {
        factory = sc.getSocketFactory();
      }
      HttpsURLConnection.setDefaultSSLSocketFactory(factory);
      HttpsURLConnection.setDefaultHostnameVerifier(NoopHostnameVerifier.INSTANCE);
      URL sslUrl = new URL("https://0.0.0.0:" + port);
      httpsURLConnection = (HttpsURLConnection) sslUrl.openConnection();
      httpsURLConnection.setDoInput(true);
      httpsURLConnection.setDoOutput(true);
      httpsURLConnection.setRequestMethod("POST");

      Transaction transaction = httpsChannel.getTransaction();
      transaction.begin();
      for (int i = 0; i < 3; i++) {
        Event event = EventBuilder.withBody("测试事件 " + (i + 1), Charset.forName("gbk"));
        httpsChannel.put(event);
      }
      transaction.commit();
      transaction.close();

      int statusCode = httpsURLConnection.getResponseCode();
      Assert.assertEquals(200, statusCode);

      Assert.assertEquals("text/plain;charset=gbk", httpsURLConnection.getContentType());

      String str = IOUtils.toString(httpsURLConnection.getInputStream(), "gbk");
      Assert.assertEquals("[测试事件 1, 测试事件 2]", str);

      Transaction tx = httpsChannel.getTransaction();
      tx.begin();
      Event e = httpsChannel.take();
      // 我们发了3条，只收了一批(2条), 所以还有 Event 在 Channel 里
      Assert.assertNotNull(e);
      tx.commit();
      tx.close();

    } finally {
      httpsURLConnection.disconnect();
    }
  }

  @Test
  public void testHttpsSinkNonHttpsClient() throws Exception {
    HttpURLConnection httpURLConnection = null;
    try {
      URL url = new URL("http://0.0.0.0:" + sslPort);
      httpURLConnection = (HttpURLConnection) url.openConnection();
      httpURLConnection.setDoInput(true);
      httpURLConnection.setDoOutput(true);
      httpURLConnection.setRequestMethod("POST");
      httpURLConnection.getResponseCode();

      Assert.fail("HTTP Client cannot connect to HTTPS source");
    } catch (Exception exception) {
      Assert.assertTrue("Exception expected", true);
    } finally {
      httpURLConnection.disconnect();
    }
  }

  private class DisabledProtocolsSocketFactory extends javax.net.ssl.SSLSocketFactory {

    private final javax.net.ssl.SSLSocketFactory socketFactory;
    private final String[] protocols;

    DisabledProtocolsSocketFactory(javax.net.ssl.SSLSocketFactory factory, String protocol) {
      this.socketFactory = factory;
      protocols = new String[1];
      protocols[0] = protocol;
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return socketFactory.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return socketFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(Socket socket, String s, int i, boolean b) throws IOException {
      SSLSocket sc = (SSLSocket) socketFactory.createSocket(socket, s, i, b);
      sc.setEnabledProtocols(protocols);
      return sc;
    }

    @Override
    public Socket createSocket(String s, int i) throws IOException, UnknownHostException {
      SSLSocket sc = (SSLSocket) socketFactory.createSocket(s, i);
      sc.setEnabledProtocols(protocols);
      return sc;
    }

    @Override
    public Socket createSocket(String s, int i, InetAddress inetAddress, int i2) throws IOException, UnknownHostException {
      SSLSocket sc = (SSLSocket) socketFactory.createSocket(s, i, inetAddress, i2);
      sc.setEnabledProtocols(protocols);
      return sc;
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i) throws IOException {
      SSLSocket sc = (SSLSocket) socketFactory.createSocket(inetAddress, i);
      sc.setEnabledProtocols(protocols);
      return sc;
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i, InetAddress inetAddress2, int i2) throws IOException {
      SSLSocket sc = (SSLSocket) socketFactory.createSocket(inetAddress, i, inetAddress2, i2);
      sc.setEnabledProtocols(protocols);
      return sc;
    }
  }

  @Test
  public void testMBeans() throws Exception {
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName = new ObjectName("org.eclipse.jetty.*:*");
    Set<ObjectInstance> queryMBeans = mbeanServer.queryMBeans(objectName, null);
    Assert.assertTrue(queryMBeans.size() > 0);
  }

}
