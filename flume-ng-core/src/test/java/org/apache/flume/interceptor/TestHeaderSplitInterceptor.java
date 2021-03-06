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

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor.Builder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

public class TestHeaderSplitInterceptor {

  private Builder fixtureBuilder;

  @Before
  public void init() throws Exception {
    fixtureBuilder = InterceptorBuilderFactory
        .newInstance(InterceptorType.HEADER_SPLIT.toString());
  }

  @Test
  public void shouldNotAllowConfigurationWithoutRegex() throws Exception {
    try {
      fixtureBuilder.build();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Pass...
    }
  }

  @Test
  public void shouldNotAllowConfigurationWithIllegalRegex() throws Exception {
    try {
      Context context = new Context();
      context.put(HeaderSplitInterceptor.REGEX, "?&?&&&?&?&?&&&??");
      fixtureBuilder.configure(context);
      fixtureBuilder.build();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Pass...
    }
  }

  @Test
  public void shouldNotAllowConfigurationWithoutMatchIds() throws Exception {
    try {
      Context context = new Context();
      context.put(HeaderSplitInterceptor.REGEX, ".*");
      context.put(HeaderSplitInterceptor.SERIALIZERS, "");
      fixtureBuilder.configure(context);
      fixtureBuilder.build();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Pass...
    }
  }

  @Test
  public void shouldNotAllowMisconfiguredSerializers() throws Exception {
    try {
      Context context = new Context();
      context.put(HeaderSplitInterceptor.REGEX, "(\\d):(\\d):(\\d)");
      context.put(HeaderSplitInterceptor.SERIALIZERS, ",,,");
      fixtureBuilder.configure(context);
      fixtureBuilder.build();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Pass...
    }
  }

  @Test
  public void shouldNotAllowEmptyNames() throws Exception {
    try {
      String space = " ";
      Context context = new Context();
      context.put(HeaderSplitInterceptor.REGEX, "(\\d):(\\d):(\\d)");
      context.put(HeaderSplitInterceptor.SERIALIZERS,
          Joiner.on(',').join(space, space, space));
      fixtureBuilder.configure(context);
      fixtureBuilder.build();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Pass...
    }
  }

  @Test
  public void shouldExtractAddHeadersForAllMatchGroups() throws Exception {
    Context context = new Context();
    context.put(HeaderSplitInterceptor.REGEX, "(\\d):(\\d):(\\d)");
    context.put(HeaderSplitInterceptor.SPLIT_KEY, "NumString");
    context.put(HeaderSplitInterceptor.SERIALIZERS, "s1 s2 s3");
    context.put(HeaderSplitInterceptor.SERIALIZERS + ".s1.name", "Num1");
    context.put(HeaderSplitInterceptor.SERIALIZERS + ".s2.name", "Num2");
    context.put(HeaderSplitInterceptor.SERIALIZERS + ".s3.name", "Num3");

    fixtureBuilder.configure(context);
    Interceptor fixture = fixtureBuilder.build();
    Map<String,String> header = new HashMap<String, String>();
    header.put("NumString", "1:2:3.4foobar5");
    
    Event event = EventBuilder.withBody("", Charsets.UTF_8,header);

    Event expected = EventBuilder.withBody("", Charsets.UTF_8,header);
    expected.getHeaders().put("Num1", "1");
    expected.getHeaders().put("Num2", "2");
    expected.getHeaders().put("Num3", "3");

    Event actual = fixture.intercept(event);

    Assert.assertArrayEquals(expected.getBody(), actual.getBody());
    Assert.assertEquals(expected.getHeaders(), actual.getHeaders());
  }

  @Test
  public void shouldExtractAddHeadersForAllMatchGroupsIgnoringMissingIds()
      throws Exception {
    Context context = new Context();
    // Skip the second group
    context.put(HeaderSplitInterceptor.REGEX,
        "^(\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d)(:\\d\\d,\\d\\d\\d)");
    context.put(HeaderSplitInterceptor.SERIALIZERS, "s1");
    context
        .put(HeaderSplitInterceptor.SERIALIZERS + ".s1.name", "timestamp");
    context.put(HeaderSplitInterceptor.SPLIT_KEY, "timeString");

    fixtureBuilder.configure(context);
    Interceptor fixture = fixtureBuilder.build();
    
    Map<String,String> header = new HashMap<String, String>();
    header.put("timeString", "2012-10-17 14:34:44,338");
    
    Event event = EventBuilder.withBody("", Charsets.UTF_8, header);
    Event expected = EventBuilder.withBody("", Charsets.UTF_8,header);
    expected.getHeaders().put("timestamp", "2012-10-17 14:34");

    Event actual = fixture.intercept(event);

    Assert.assertArrayEquals(expected.getBody(), actual.getBody());
    Assert.assertEquals(expected.getHeaders(), actual.getHeaders());

  }
}
