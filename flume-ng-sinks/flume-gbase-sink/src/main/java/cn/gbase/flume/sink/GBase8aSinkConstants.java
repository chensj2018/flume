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

import com.google.common.base.Charsets;

/**
 * GBase8aSinkConstants
 * 
 * @author He Jiang
 *
 */
public class GBase8aSinkConstants {

  public static final String GBASE8A_PREFIX = "gbase8a.";

  /* 8a 连接参数 */
  public static final String CONNECTION_STRING = GBASE8A_PREFIX + "jdbcUrl";
  public static final String CONNECTION_USERNAME = GBASE8A_PREFIX + "username";
  public static final String CONNECTION_PASSWORD = GBASE8A_PREFIX + "password";
  public static final String CONNECTION_DRIVER_CLASS = GBASE8A_PREFIX + "driverClass";
  
  /* 8a 加载语句 (直接设置 SQL, 同时支持自动替换 ${localhost}为本机ip+http服务的port) */
  public static final String SQL_STRING = GBASE8A_PREFIX + "loadSql";
  /* 8a 加载语句的执行间隔(单位:秒)；只有在flume channel空闲是有效*/
  public static final String LOAD_INTERVAL = GBASE8A_PREFIX + "loadInterval";
  
  /* handler 参数 (设置时需要加 "handler." 前缀) */
  public static final String BATCH_SIZE = "batchSize";
  public static final String CHARACTER_ENCODING = "characterEncoding";
  public static final String CONTENT_TYPE = "contentType";
  public static final String CONTENT_PREFIX = "contentPrefix";
  public static final String CONTENT_SURFFIX = "contentSurffix";
  public static final String CONTENT_SEPARATOR = "contentSeparator";

  /* 参数默认值 */
  public static final int DFLT_INTERVAL = 10000; // ms
  public static final String DFLT_HANDLER = "cn.gbase.flume.sink.PassiveHttpSinkBlobHandler";
  public static final String DFLT_DRIVER_CLASS = "com.gbase.jdbc.Driver";
  public static final int DFLT_LOAD_INTERVAL = 5; // s

  /* handler 参数默认值 */
  public static final int DFLT_BATCH_SIZE = 10000; // events
  public static final String DFLT_CHARACTER_ENCODING = Charsets.UTF_8.name();
  public static final String DFLT_CONTENT_TYPE = "application/json";
  public static final String DFLT_CONTENT_PREFIX = null;
  public static final String DFLT_CONTENT_SURFFIX = null;
  public static final String DFLT_CONTENT_SEPARATOR = null;
}
