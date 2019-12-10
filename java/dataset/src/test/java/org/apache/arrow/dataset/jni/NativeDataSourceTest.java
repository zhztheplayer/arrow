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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.dataset.datasource.DataSource;
import org.apache.arrow.dataset.datasource.DataSourceDiscovery;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSetDataSourceDiscovery;
import org.apache.arrow.dataset.file.FileSystem;
import org.apache.arrow.dataset.filter.Filter;
import org.apache.arrow.dataset.fragment.DataFragment;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

public class NativeDataSourceTest {

  private String sampleParquet() {
    return NativeDataSourceTest.class.getResource(File.separator + "users.parquet").getPath();
  }

  private void testDiscoveryEndToEnd(DataSourceDiscovery discovery) {
    Schema schema = discovery.inspect();

    Assert.assertEquals("Schema<name: Utf8 not null, favorite_color: Utf8, favorite_numbers: List<array: " +
        "Int(32, true) not null> not null>(metadata: {avro.schema={\"type\":\"record\",\"name\":\"User\"," +
        "\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}," +
        "{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]},{\"name\":\"favorite_numbers\"," +
        "\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}})", schema.toString());

    DataSource dataSource = discovery.finish();
    Assert.assertNotNull(dataSource);

    List<? extends DataFragment> fragments = collect(
        dataSource.getFragments(new ScanOptions(new String[0], Filter.EMPTY)));
    Assert.assertEquals(2, fragments.size());

    DataFragment fragment = fragments.get(0);
    List<? extends ScanTask> scanTasks = collect(fragment.scan());
    Assert.assertEquals(2, scanTasks.size());

    ScanTask scanTask = scanTasks.get(0);
    List<? extends VectorSchemaRoot> data = collect(scanTask.scan()); // fixme it seems c++ parquet reader doesn't create buffers for list field it self, as a result Java side buffer pointer gets out of bound.
    Assert.assertNotNull(data);
  }

  @Test
  public void testLocalFs() {
    String path = sampleParquet();
    DataSourceDiscovery discovery = new FileSetDataSourceDiscovery(
        new RootAllocator(Long.MAX_VALUE), FileFormat.PARQUET, FileSystem.LOCAL,
        Collections.singletonList(path));
    testDiscoveryEndToEnd(discovery);
  }

  @Test
  public void testHdfs() {
    String path = "file:" + sampleParquet();
    DataSourceDiscovery discovery = new FileSetDataSourceDiscovery(
        new RootAllocator(Long.MAX_VALUE), FileFormat.PARQUET, FileSystem.HDFS,
        Collections.singletonList(path));
    testDiscoveryEndToEnd(discovery);
  }

  public void testScanner() {
    // todo
  }

  public void testFilter() {
    // todo
  }

  public void testProjector() {
    // todo
  }

  private <T> List<T> collect(Iterable<T> iterable) {
    return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
  }
}
