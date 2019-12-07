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

import org.apache.arrow.dataset.datasource.DataSource;
import org.apache.arrow.dataset.datasource.DataSourceDiscovery;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NativeDataSourceDiscovery implements DataSourceDiscovery, AutoCloseable {
  private final long dataSourceDiscoveryId;
  private final BufferAllocator allocator;

  public NativeDataSourceDiscovery(BufferAllocator allocator, long dataSourceDiscoveryId) {
    this.allocator = allocator;
    this.dataSourceDiscoveryId = dataSourceDiscoveryId;
  }
  @Override
  public Schema inspect() {
    byte[] buffer = JniWrapper.get().inspectSchema(dataSourceDiscoveryId);
    try (MessageChannelReader schemaReader =
             new MessageChannelReader(
                 new ReadChannel(
                     new ByteArrayReadableSeekableByteChannel(buffer)), allocator)) {

      MessageResult result = schemaReader.readNext();
      if (result == null) {
        throw new IOException("Unexpected end of input. Missing schema.");
      }

      return MessageSerializer.deserializeSchema(result.getMessage());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DataSource finish() {
    return new NativeDataSource(new NativeContext(inspect(), allocator),
      JniWrapper.get().createDataSource(dataSourceDiscoveryId));
  }

  @Override
  public void close() throws Exception {
    JniWrapper.get().closeDataSourceDiscovery(dataSourceDiscoveryId);
  }
}
