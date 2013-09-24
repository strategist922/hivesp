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
package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
//import java.sql.Timestamp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.io.GeometryWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyGeometryObjectInspector;

/**
 *
 * LazyGeometry
 * Serializes and deserializes a Geometry in the JDBC geometry format
 *
 */
public class LazyGeometry extends LazyPrimitive<LazyGeometryObjectInspector, GeometryWritable> {
  static final private Log LOG = LogFactory.getLog(LazyGeometry.class);

  public LazyGeometry(LazyGeometryObjectInspector oi) {
    super(oi);
    data = new GeometryWritable();
  }

  public LazyGeometry(LazyGeometry copy) {
    super(copy);
    data = new GeometryWritable(copy.data.get());
  }

  /**
   * Initilizes LazyGeometry object by interpreting the input bytes
   * as a JDBC geometry string
   *
   * @param bytes
   * @param start
   * @param length
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    String s = null;
    try {
      s = new String(bytes.getData(), start, length, "US-ASCII");
    } catch (UnsupportedEncodingException e) {
      LOG.error(e);
      s = "";
    }

    if (s.compareTo("NULL") == 0) {
      logExceptionMessage(bytes, start, length, "GEOMETRY");
    }
    data.set(s);
  }

  @Override
  public GeometryWritable getWritableObject() {
    return data;
  }
}
