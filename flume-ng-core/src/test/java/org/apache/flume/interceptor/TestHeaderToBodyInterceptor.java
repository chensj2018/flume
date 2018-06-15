/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.interceptor;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor.Builder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHeaderToBodyInterceptor {
  private Builder fixtureBuilder;
  private static Gson gson = new Gson();
  
  @Before
  public void init() throws Exception {
    fixtureBuilder = InterceptorBuilderFactory
        .newInstance(InterceptorType.HEADER_TO_BODY.toString());
  }

  @Test
  public void testAllowedBodyTypeJson() throws Exception {
    Context context = new Context();
    context.put("bodyType", "json");
    context.put("headerToBody", "topic:string:kafka_topic");
    fixtureBuilder.configure(context);
    fixtureBuilder.build();
  }

  @Test
  public void testAllowedBodyTypeCsv() throws Exception {
    Context context = new Context();
    context.put("bodyType", "csv");
    context.put("headerToBody", "topic:string:kafka_topic");
    fixtureBuilder.configure(context);
    fixtureBuilder.build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalBodyType() throws Exception {
    Context context = new Context();
    context.put("bodyType", "unknow");
    context.put("headerToBody", "topic:string:kafka_topic");
    fixtureBuilder.configure(context);
    fixtureBuilder.build();
  }

  @Test
  public void testAllowedHeaderToBody() throws Exception {
    Context context = new Context();
    context.put("bodyType", "json");
    context.put("headerToBody", "topic:string:kafka_topic");
    fixtureBuilder.configure(context);
    fixtureBuilder.build();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testIllegalHeaderToBody() throws Exception {
    Context context = new Context();
    context.put("bodyType", "unknow");
    context.put("headerToBody", "topic:string");
    fixtureBuilder.configure(context);
    fixtureBuilder.build();
  }
  
  @Test
  public void testBasicJson() throws Exception {
    Context context = new Context();
    context.put("bodyType", "json");
    context.put("headerToBody", "topic:string:kafka_topic");
    fixtureBuilder.configure(context);
    Interceptor fixture = fixtureBuilder.build();
    
    Event event = EventBuilder.withBody("{\"id\":\"1111111111\",\"name\":\"jack\"}",
        Charsets.UTF_8);
    event.getHeaders().put("topic", "myTopic");

    Event actual = fixture.intercept(event);
    Map<String, Object> actualPayloadMap = gson.fromJson(new String(actual.getBody(), "utf-8"),
        Map.class);

    Event expected = EventBuilder.withBody(
        "{\"id\":\"1111111111\",\"name\":\"jack\",\"kafka_topic\":\"myTopic\"}", Charsets.UTF_8);
    Map<String, Object> expectedPayloadMap = gson.fromJson(new String(expected.getBody(), "utf-8"),
        Map.class);
    
    Assert.assertTrue(actualPayloadMap.equals(expectedPayloadMap));
  }
  
  @Test
  public void testBasicCsv() throws Exception {
    Context context = new Context();
    context.put("bodyType", "csv");
    context.put("headerToBody", "topic:string:0");
    fixtureBuilder.configure(context);
    Interceptor fixture = fixtureBuilder.build();
    
    Event event = EventBuilder.withBody("1111111111,jack",
        Charsets.UTF_8);
    event.getHeaders().put("topic", "myTopic");
    
    Event actual = fixture.intercept(event);
    
    Event expected = EventBuilder.withBody("myTopic,1111111111,jack",
        Charsets.UTF_8);
    
    Assert.assertArrayEquals(expected.getBody(), actual.getBody());
  }
  
  @Test
  public void testBasicJsonHeaderToBodyTimestamp() throws Exception {
    Context context = new Context();
    context.put("bodyType", "json");
    context.put("headerToBody", "flume_timestamp:timestamp:flume_timestamp");
    fixtureBuilder.configure(context);
    Interceptor fixture = fixtureBuilder.build();
    
    Event event = EventBuilder.withBody("{\"id\":\"1111111111\",\"name\":\"jack\"}",
        Charsets.UTF_8);
    event.getHeaders().put("flume_timestamp", "1357872222598");

    Event actual = fixture.intercept(event);
    Map<String, Object> actualPayloadMap = gson.fromJson(new String(actual.getBody(), "utf-8"),
        Map.class);

    Event expected = EventBuilder.withBody(
        "{\"id\":\"1111111111\",\"name\":\"jack\",\"flume_timestamp\":\"2013-01-11 10:43:42.598\"}",
        Charsets.UTF_8);
    Map<String, Object> expectedPayloadMap = gson.fromJson(new String(expected.getBody(), "utf-8"),
        Map.class);
    
    Assert.assertTrue(actualPayloadMap.equals(expectedPayloadMap));
  }
  
  @Test
  public void testBasicJsonHeaderToBodyDate() throws Exception {
    Context context = new Context();
    context.put("bodyType", "json");
    context.put("headerToBody", "flume_timestamp:date:flume_time");
    fixtureBuilder.configure(context);
    Interceptor fixture = fixtureBuilder.build();
    
    Event event = EventBuilder.withBody("{\"id\":\"1111111111\",\"name\":\"jack\"}",
        Charsets.UTF_8);
    event.getHeaders().put("flume_timestamp", "1357872222598");

    Event actual = fixture.intercept(event);
    Map<String, Object> actualPayloadMap = gson.fromJson(new String(actual.getBody(), "utf-8"),
        Map.class);

    Event expected = EventBuilder.withBody(
        "{\"id\":\"1111111111\",\"name\":\"jack\",\"flume_time\":\"2013-01-11\"}",
        Charsets.UTF_8);
    Map<String, Object> expectedPayloadMap = gson.fromJson(new String(expected.getBody(), "utf-8"),
        Map.class);
    
    Assert.assertTrue(actualPayloadMap.equals(expectedPayloadMap));
  }
}
