/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.dataset.jni;

import java.util.Arrays;
import java.util.List;

public class NativeRecordBatchHandle {

  private final long numRows;
  private final List<Field> fields;
  private final List<Buffer> buffers;

  public NativeRecordBatchHandle(long numRows, Field[] fields, Buffer[] buffers) {
    this.numRows = numRows;
    this.fields = Arrays.asList(fields);
    this.buffers = Arrays.asList(buffers);
  }

  public long getNumRows() {
    return numRows;
  }

  public List<Field> getFields() {
    return fields;
  }

  public List<Buffer> getBuffers() {
    return buffers;
  }

  public static class Field {
    public final long length;
    public final long nullCount;

    public Field(long length, long nullCount) {
      this.length = length;
      this.nullCount = nullCount;
    }
  }

  public static class Buffer {
    public final long nativeInstanceId;
    public final long memoryAddress;
    public final long size;
    public final long capacity;

    public Buffer(long nativeInstanceId, long memoryAddress, long size, long capacity) {
      this.nativeInstanceId = nativeInstanceId;
      this.memoryAddress = memoryAddress;
      this.size = size;
      this.capacity = capacity;
    }
  }
}
