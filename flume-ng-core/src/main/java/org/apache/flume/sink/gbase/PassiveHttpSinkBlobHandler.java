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

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.eclipse.jetty.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * 
 * @author He Jiang
 *
 */
public class PassiveHttpSinkBlobHandler implements PassiveHttpSinkHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PassiveHttpSinkBlobHandler.class);
  private Sink sink;
  private CounterGroup counterGroup = new CounterGroup();
  private int batchSize = GBase8aSinkConstants.DFLT_BATCH_SIZE;
  private String contentType = GBase8aSinkConstants.DFLT_CONTENT_TYPE;
  private String characterEncoding = GBase8aSinkConstants.DFLT_CHARACTER_ENCODING;
  private byte[] contentPrefix = null;
  private byte[] contentSurffix = null;
  private byte[] contentSeparator = null;

  @Override
  public long handle(HttpServletRequest request, HttpServletResponse response)
      throws EventDeliveryException {
    checkRequest(request);

    Channel channel = sink.getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;

    if ("HEAD".equals(request.getMethod())) {
      setupResponse(response);
      return 0;
    }

    long eventSize = 0;
    try {
      transaction.begin();
      OutputStream stream = response.getOutputStream();
      for (eventSize = 0; eventSize < batchSize; eventSize++) {
        event = channel.take();
        if (event == null) {
          break;
        }

        if (eventSize == 0) {
          setupResponse(response);
          if (contentPrefix != null) {
            stream.write(contentPrefix);
          }
        } else {
          if (contentSeparator != null) {
            stream.write(contentSeparator);
          }
        }

        // write event body to response
        stream.write(event.getBody());
      }

      transaction.commit();
      counterGroup.addAndGet("events.success", (long) Math.min(batchSize, eventSize));
      counterGroup.incrementAndGet("transaction.success");
      LOG.info("{} events to deliver.", eventSize);
      if (eventSize > 0) {
        if (contentSurffix != null) {
          stream.write(contentSurffix);
        }
      } else {
        response.sendError(HttpServletResponse.SC_NO_CONTENT, "No event to deliver.");
      }
    } catch (Exception ex) {
      transaction.rollback();
      counterGroup.incrementAndGet("transaction.failed");
      LOG.error("Failed to deliver event. Exception follows.", ex);
      throw new EventDeliveryException("Failed to deliver event: " + event, ex);
    } finally {
      transaction.close();
    }

    return eventSize;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
   */
  @Override
  public void configure(Context context) {
    batchSize = context.getInteger(GBase8aSinkConstants.BATCH_SIZE, GBase8aSinkConstants.DFLT_BATCH_SIZE);
    characterEncoding = context.getString(GBase8aSinkConstants.CHARACTER_ENCODING, GBase8aSinkConstants.DFLT_CHARACTER_ENCODING);
    contentType = context.getString(GBase8aSinkConstants.CONTENT_TYPE, GBase8aSinkConstants.DFLT_CONTENT_TYPE);
    
    String prefix = context.getString(GBase8aSinkConstants.CONTENT_PREFIX, GBase8aSinkConstants.DFLT_CONTENT_PREFIX);
    if (!Strings.isNullOrEmpty(prefix)) {
      try {
        contentPrefix = StringEscapeUtils.unescapeJava(prefix).getBytes(characterEncoding);
      } catch (UnsupportedEncodingException e) {
        LOG.error("Invalid contentPrefix", e);
      }
    }

    String surffix = context.getString(GBase8aSinkConstants.CONTENT_SURFFIX, GBase8aSinkConstants.DFLT_CONTENT_SURFFIX);
    if (!Strings.isNullOrEmpty(surffix)) {
      try {
        contentSurffix = StringEscapeUtils.unescapeJava(surffix).getBytes(characterEncoding);
      } catch (UnsupportedEncodingException e) {
        LOG.error("Invalid contentSurffix", e);
      }
    }  
    String separator = context.getString(GBase8aSinkConstants.CONTENT_SEPARATOR, GBase8aSinkConstants.DFLT_CONTENT_SEPARATOR);
    if (!Strings.isNullOrEmpty(separator)) {
      try {
        contentSeparator = StringEscapeUtils.unescapeJava(separator).getBytes(characterEncoding);
      } catch (UnsupportedEncodingException e) {
        LOG.error("Invalid contentSeparator", e);
      }
    }

    counterGroup.setName(sink.getName());
  }

  @Override
  public void setSink(Sink sink) {
    this.sink = sink;
  }

  private void checkRequest(HttpServletRequest request) {
    // throw HTTPBadRequestException if an invalid request received
  }

  private void setupResponse(HttpServletResponse response) {
    response.setContentType(contentType);
    response.setCharacterEncoding(characterEncoding);
    response.setStatus(HttpServletResponse.SC_OK);
  }
}
