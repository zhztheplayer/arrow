//
// Created by root on 11/26/19.
//

#include <arrow/dataset/file_base.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/hdfs.h>
#include <arrow/ipc/api.h>
#include <arrow/util/iterator.h>
#include "jni/concurrent_map.h"

#include "org_apache_arrow_dataset_file_JniWrapper.h"
#include "org_apache_arrow_dataset_jni_JniWrapper.h"

static jclass illegal_access_exception_class;
static jclass illegal_argument_exception_class;
static jclass runtime_exception_class;

static jclass record_batch_handle_class;
static jclass record_batch_handle_field_class;
static jclass record_batch_handle_buffer_class;

static jmethodID record_batch_handle_constructor;
static jmethodID record_batch_handle_field_constructor;
static jmethodID record_batch_handle_buffer_constructor;

static jint JNI_VERSION = JNI_VERSION_1_6;

using arrow::jni::ConcurrentMap;

static ConcurrentMap<arrow::dataset::DataSourceDiscoveryPtr > data_source_discovery_holder_;
static ConcurrentMap<arrow::dataset::DataSourcePtr> data_source_holder_;
static ConcurrentMap<arrow::dataset::DataFragmentPtr > data_fragment_holder_;
static ConcurrentMap<arrow::dataset::ScanTaskPtr> scan_task_holder_;
static ConcurrentMap<std::shared_ptr<arrow::RecordBatchIterator>> iterator_holder_;
static ConcurrentMap<std::shared_ptr<arrow::Buffer>> buffer_holder_;

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  return global_class;
}

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
        " within signature" + std::string(sig);
    env->ThrowNew(illegal_access_exception_class, error_message.c_str());
  }
  return ret;
}

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  illegal_access_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");
  runtime_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");

  record_batch_handle_class =
      CreateGlobalClassReference(env, "Lorg.apache.arrow.dataset.jni/NativeRecordBatchHandle;");
  record_batch_handle_field_class =
      CreateGlobalClassReference(env, "Lorg.apache.arrow.dataset.jni/NativeRecordBatchHandle$Field;");
  record_batch_handle_buffer_class =
      CreateGlobalClassReference(env, "Lorg.apache.arrow.dataset.jni/NativeRecordBatchHandle$Buffer;");

  record_batch_handle_constructor = GetMethodID(env, record_batch_handle_class, "<init>",
                                                "(J[Lorg.apache.arrow.dataset.jni/NativeRecordBatchHandle$Field;"
                                                "[Lorg.apache.arrow.dataset.jni/NativeRecordBatchHandle$Buffer;)V");
  record_batch_handle_field_constructor = GetMethodID(env, record_batch_handle_field_class, "<init>",
                                                      "(JJ)V");
  record_batch_handle_buffer_constructor = GetMethodID(env, record_batch_handle_buffer_class, "<init>",
                                                       "(JJJJ)V");

  env->ExceptionDescribe();

  return JNI_VERSION;
}


void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);
  env->DeleteGlobalRef(runtime_exception_class);
  env->DeleteGlobalRef(record_batch_handle_class);
  env->DeleteGlobalRef(record_batch_handle_field_class);
  env->DeleteGlobalRef(record_batch_handle_buffer_class);

  data_source_discovery_holder_.Clear();
  data_source_holder_.Clear();
  data_fragment_holder_.Clear();
  scan_task_holder_.Clear();
  iterator_holder_.Clear();
}

std::shared_ptr<arrow::Schema> SchemaFromColumnNames(
    const std::shared_ptr<arrow::Schema>& input, const std::vector<std::string>& column_names) {
  std::vector<std::shared_ptr<arrow::Field>> columns;
  for (const auto& name : column_names) {
    columns.push_back(input->GetFieldByName(name));
  }
  return std::make_shared<arrow::Schema>(columns);
}

std::shared_ptr<arrow::dataset::FileFormat> GetFileFormat(JNIEnv *env, jint id) {
  switch (id) {
    case 0:
      return std::make_shared<arrow::dataset::ParquetFileFormat>();
    default:
      std::string error_message = "illegal file format id: " + std::to_string(id);
      env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
      return nullptr; // unreachable
  }
}

std::shared_ptr<arrow::fs::FileSystem> GetFileSystem(JNIEnv *env, jint id) {
  switch (id) {
    case 0:
      return std::make_shared<arrow::fs::LocalFileSystem>();
    // case 1:
    //  return std::make_shared<arrow::fs::HadoopFileSystem>(); fixme passing with config
    default:
      std::string error_message = "illegal file format id: " + std::to_string(id);
      env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
      return nullptr; // unreachable
  }
}

std::string JStringToCString(JNIEnv* env, jstring string) {
  int32_t jlen, clen;
  clen = env->GetStringUTFLength(string);
  jlen = env->GetStringLength(string);
  std::vector<char> buffer(clen);
  env->GetStringUTFRegion(string, 0, jlen, buffer.data());
  return std::string(buffer.data(), clen);
}

std::vector<std::string> ToStringVector(JNIEnv* env, jobjectArray& str_array) {
  int length = env->GetArrayLength(str_array);
  std::vector<std::string> vector;
  for (int i = 0; i < length; i++) {
    auto string = (jstring) (env->GetObjectArrayElement(str_array, i));
    vector.push_back(JStringToCString(env, string));
  }
  return vector;
}

template <typename T>
std::vector<T> collect(arrow::Iterator<T> itr) {
  std::vector<T> vector;
  T t;
  do {
    auto status = itr.Next(&t);
    if (!status.ok()) {
      return std::vector<T>(); // fixme
    }
    vector.push_back(t);
  } while (t);
  return vector;
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeDataSourceDiscovery
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeDataSourceDiscovery
    (JNIEnv *, jobject, jlong id) {
  data_source_discovery_holder_.Erase(id);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    inspectSchema
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_inspectSchema
    (JNIEnv* env, jobject, jlong data_source_discovery_id) {
  std::shared_ptr<arrow::dataset::DataSourceDiscovery> d
      = data_source_discovery_holder_.Lookup(data_source_discovery_id);
  std::shared_ptr<arrow::Schema> schema = d->schema();

  std::shared_ptr<arrow::Buffer> out;

  auto status =
      arrow::ipc::SerializeSchema(*schema, nullptr, arrow::default_memory_pool(), &out);
  if (!status.ok()) {
    std::string error_message = "unable to serialize schema";
    env->ThrowNew(runtime_exception_class, error_message.c_str());
  }

  jbyteArray ret = env->NewByteArray(out->size());
  auto src = reinterpret_cast<const jbyte *>(out->data());
  env->SetByteArrayRegion(ret, 0, out->size(), src);
  return ret;
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    createDataSource
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_createDataSource
    (JNIEnv *, jobject, jlong data_source_discovery_id) {
  std::shared_ptr<arrow::dataset::DataSourceDiscovery> d
      = data_source_discovery_holder_.Lookup(data_source_discovery_id);
  arrow::dataset::DataSourcePtr data_source = d->Finish().ValueOrDie();// fixme ValueOrDie
  return data_source_holder_.Insert(data_source);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeDataSource
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeDataSource
    (JNIEnv *, jobject, jlong id) {
  data_source_holder_.Erase(id);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    getFragments
 * Signature: (J[Ljava/lang/String;[B)[J
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_getFragments
    (JNIEnv* env, jobject, jlong data_source_id, jobjectArray columns, jbyteArray filter) {
  // todo consider filter buffer (the last param) which is currently ignored
  arrow::dataset::DataSourcePtr data_source = data_source_holder_.Lookup(data_source_id);
  arrow::dataset::ScanOptionsPtr scan_options = arrow::dataset::ScanOptions::Defaults();
  std::vector<std::string> column_names = ToStringVector(env, columns);
  arrow::dataset::DataFragmentIterator itr = data_source->GetFragments(scan_options);
  std::vector<arrow::dataset::DataFragmentPtr> vector = collect(std::move(itr));
  jlongArray ret = env->NewLongArray(vector.size());
  for (unsigned long i = 0; i < vector.size(); i++) {
    arrow::dataset::DataFragmentPtr data_fragment = vector.at(i);
    jlong id[] = {data_fragment_holder_.Insert(data_fragment)};
    env->SetLongArrayRegion(ret, i, i + 1, id);
  }
  return ret;
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeFragment
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeFragment
    (JNIEnv *, jobject, jlong id) {
  data_fragment_holder_.Erase(id);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    getScanTasks
 * Signature: (J)[J
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_getScanTasks
    (JNIEnv* env, jobject, jlong fragment_id) {
  arrow::dataset::DataFragmentPtr data_fragment = data_fragment_holder_.Lookup(fragment_id);
  arrow::dataset::ScanTaskIterator itr = data_fragment->Scan(std::make_shared<arrow::dataset::ScanContext>())
      .ValueOrDie(); // fixme ValueOrDie
  std::vector<arrow::dataset::ScanTaskPtr> vector = collect(std::move(itr));
  jlongArray ret = env->NewLongArray(vector.size());
  for (unsigned long i = 0; i < vector.size(); i++) {
    arrow::dataset::ScanTaskPtr scan_task = vector.at(i);
    jlong id[] = {scan_task_holder_.Insert(scan_task)};
    env->SetLongArrayRegion(ret, i, i + 1, id);
  }
  return ret;
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeScanTask
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeScanTask
    (JNIEnv *, jobject, jlong id) {
  scan_task_holder_.Erase(id);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    scan
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_scan
    (JNIEnv *, jobject, jlong scan_task_id) {
  arrow::dataset::ScanTaskPtr scan_task = scan_task_holder_.Lookup(scan_task_id);
  arrow::RecordBatchIterator record_batch_iterator = scan_task->Scan().ValueOrDie(); // fixme ValueOrDie
  return iterator_holder_
      .Insert(std::make_shared<arrow::RecordBatchIterator>(std::move(record_batch_iterator))); // move and propagate
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeIterator
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeIterator
    (JNIEnv *, jobject, jlong id) {
  iterator_holder_.Erase(id);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    nextRecordBatch
 * Signature: (J)Lorg/apache/arrow/dataset/jni/NativeRecordBatchHandle;
 */
JNIEXPORT jobject JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_nextRecordBatch
    (JNIEnv* env, jobject, jlong iterator_id) {
  std::shared_ptr<arrow::RecordBatchIterator> itr = iterator_holder_.Lookup(iterator_id);
  std::shared_ptr<arrow::RecordBatch> record_batch;
  auto status = itr->Next(&record_batch);
  if (!status.ok()) {
    return nullptr; // fixme throw an error
  }
  if (record_batch == nullptr) {
    return nullptr; // stream ended
  }
  std::shared_ptr<arrow::Schema> schema = record_batch->schema();
  jobjectArray field_array =
      env->NewObjectArray(schema->num_fields(), record_batch_handle_field_class, nullptr);

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto column = record_batch->column(i);
    auto dataArray = column->data();
    jobject field = env->NewObject(record_batch_handle_field_class, record_batch_handle_field_constructor,
                                   column->length(), column->null_count());
    env->SetObjectArrayElement(field_array, i, field);

    for (auto& buffer : dataArray->buffers) {
      buffers.push_back(buffer);
    }
  }

  jobjectArray buffer_array =
      env->NewObjectArray(buffers.size(), record_batch_handle_buffer_class, nullptr);

  for (size_t j = 0; j < buffers.size(); ++j) {
    auto buffer = buffers[j];
    jobject buffer_handle = env->NewObject(record_batch_handle_buffer_class, record_batch_handle_buffer_constructor,
                                           buffer_holder_.Insert(buffer), buffer->data(),
                                           buffer->size(), buffer->capacity());
    env->SetObjectArrayElement(buffer_array, j, buffer_handle);
  }

  jobject ret = env->NewObject(record_batch_handle_class, record_batch_handle_constructor,
                               record_batch->num_rows(), field_array, buffer_array);
  return ret;
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    releaseBuffer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_releaseBuffer
    (JNIEnv *, jobject, jlong id) {
  buffer_holder_.Erase(id);
}

/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    makeFileSetDataSourceDiscovery
 * Signature: ([Ljava/lang/String;II)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_file_JniWrapper_makeFileSetDataSourceDiscovery
    (JNIEnv* env, jobject, jobjectArray paths, jint file_format_id, jint file_system_id) {
  std::shared_ptr<arrow::dataset::FileFormat> file_format = GetFileFormat(env, file_format_id);
  arrow::fs::FileSystemPtr fs = GetFileSystem(env, file_system_id);
  std::vector<std::string> path_vector = ToStringVector(env, paths);
  std::shared_ptr<arrow::dataset::DataSourceDiscovery>
      d = arrow::dataset::FileSetDataSourceDiscovery::Make(path_vector, fs, file_format).ValueOrDie();// fixme ValueOrDie
  return data_source_discovery_holder_.Insert(d);
}