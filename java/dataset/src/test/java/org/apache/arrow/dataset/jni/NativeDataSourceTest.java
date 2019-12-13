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
import java.util.Collections;
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
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

public class NativeDataSourceTest {

  private String sampleParquet() {
    return NativeDataSourceTest.class.getResource(File.separator + "userdata1.parquet").getPath();
  }

  private void testDiscoveryEndToEnd(DataSourceDiscovery discovery) {
    Schema schema = discovery.inspect();

    Assert.assertEquals("Schema<registration_dttm: Timestamp(NANOSECOND, null), id: Int(32, true), " +
        "first_name: Utf8, last_name: Utf8, email: Utf8, gender: Utf8, ip_address: Utf8, cc: Utf8, " +
        "country: Utf8, birthdate: Utf8, salary: FloatingPoint(DOUBLE), title: Utf8, comments: Utf8>",
        schema.toString());

    DataSource dataSource = discovery.finish();
    Assert.assertNotNull(dataSource);

    List<? extends DataFragment> fragments = collect(
        dataSource.getFragments(new ScanOptions(new String[0], Filter.EMPTY, 100)));
    Assert.assertEquals(1, fragments.size());

    DataFragment fragment = fragments.get(0);
    List<? extends ScanTask> scanTasks = collect(fragment.scan());
    Assert.assertEquals(1, scanTasks.size());

    ScanTask scanTask = scanTasks.get(0);
    List<? extends VectorSchemaRoot> data = collect(scanTask.scan());
    Assert.assertNotNull(data);
    // 1000 rows total in file userdata1.parquet
    Assert.assertEquals(10, data.size());
    VectorSchemaRoot vsr = data.get(0);
    Assert.assertEquals(100, vsr.getRowCount());


    // FIXME when using list field:
    // FIXME it seems c++ parquet reader doesn't create buffers for list field it self,
    // FIXME as a result Java side buffer pointer gets out of bound.
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
  public void testHdfsWithFileProtocol() {
    String path = "file:" + sampleParquet();
    DataSourceDiscovery discovery = new FileSetDataSourceDiscovery(
        new RootAllocator(Long.MAX_VALUE), FileFormat.PARQUET, FileSystem.HDFS,
        Collections.singletonList(path));
    testDiscoveryEndToEnd(discovery);
  }

  @Test
  public void testHdfsWithHdfsProtocol() {
    // todo
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
