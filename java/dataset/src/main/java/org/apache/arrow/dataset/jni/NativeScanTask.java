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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.memory.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class NativeScanTask implements ScanTask, AutoCloseable {
  private final NativeContext context;
  private final long scanTaskId;

  public NativeScanTask(NativeContext context, long scanTaskId) {
    this.context = context;
    this.scanTaskId = scanTaskId;
  }

  @Override
  public Iterable<? extends VectorSchemaRoot> scan() {

    return () -> new Iterator<VectorSchemaRoot>() {
      Reader in = new Reader(JniWrapper.get().scan(scanTaskId));
      VectorSchemaRoot peek = null;

      @Override
      public boolean hasNext() {
        try {
          if (peek != null) {
            return true;
          }
          if (!in.loadNextBatch()) {
            return false;
          }
          peek = in.getVectorSchemaRoot();
          return true;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public VectorSchemaRoot next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        try {
          return peek;
        } finally {
          peek = null;
        }
      }
    };
  }

  @Override
  public void close() {
    JniWrapper.get().closeScanTask(scanTaskId);
  }

  private class Reader extends ArrowReader {

    private final long recordBatchIteratorId;

    Reader(long recordBatchIteratorId) {
      super(context.getAllocator());
      this.recordBatchIteratorId = recordBatchIteratorId;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      ensureInitialized(); // fixme it seems that the initialization is not thread-safe. Does caller already make it safe?
      NativeRecordBatchHandle handle = JniWrapper.get().nextRecordBatch(recordBatchIteratorId);
      if (handle == null) {
        return false;
      }
      final ArrayList<ArrowBuf> buffers = new ArrayList<>();
      for (NativeRecordBatchHandle.Buffer buffer : handle.getBuffers()) {
        final BaseAllocator allocator = context.getAllocator();
        final NativeUnderlingMemory am = new NativeUnderlingMemory(allocator,
            (int) buffer.size, buffer.nativeInstanceId);
        final ReferenceManager rm = am.associate(allocator); // here we have ref count of 1
        ArrowBuf buf = new ArrowBuf(rm, null, (int) buffer.size, buffer.memoryAddress, false);
        buffers.add(buf);
      }
      loadRecordBatch(
        new ArrowRecordBatch((int) handle.getNumRows(), handle.getFields().stream()
          .map(field -> new ArrowFieldNode((int) field.length, (int) field.nullCount))
          .collect(Collectors.toList()), buffers));
      return true;
    }

    @Override
    public long bytesRead() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void closeReadSource() throws IOException {
      JniWrapper.get().closeIterator(recordBatchIteratorId);
    }

    @Override
    protected Schema readSchema() throws IOException {
      return context.getSchema();
    }


  }
}
