//
// Created by root on 11/26/19.
//

#include <arrow/dataset/file_base.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/hdfs.h>
#include <arrow/ipc/api.h>
#include "jni/concurrent_map.h"

#include "org_apache_arrow_dataset_file_JniWrapper.h"
#include "org_apache_arrow_dataset_jni_JniWrapper.h"

static jclass io_exception_class;
static jclass illegal_access_exception_class;
static jclass illegal_argument_exception_class;
static jclass runtime_exception_class;

static jint JNI_VERSION = JNI_VERSION_1_6;

using arrow::jni::ConcurrentMap;

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  return global_class;
}

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  io_exception_class = CreateGlobalClassReference(env, "Ljava/io/IOException;");
  illegal_access_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");
  runtime_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");

  env->ExceptionDescribe();

  return JNI_VERSION;
}

std::shared_ptr<arrow::dataset::FileFormat> GetFileFormat(JNIEnv *env, jint id) {
  switch (id) {
    case 0:
      return std::make_shared<arrow::dataset::ParquetFileFormat>();
    default:
      std::string error_message = "illegal file format id: " + std::to_string(id);
      env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
}

std::shared_ptr<arrow::fs::FileSystem> GetFileSystem(JNIEnv *env, jint id) {
  switch (id) {
    case 0:
      return std::make_shared<arrow::fs::LocalFileSystem>();
    case 1:
      return std::make_shared<arrow::fs::HadoopFileSystem>(); // fixme config
    default:
      std::string error_message = "illegal file format id: " + std::to_string(id);
      env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
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

std::vector<std::string> ToStringVector(JNIEnv *env, jobjectArray str_array) {
  int length = env->GetArrayLength(str_array);
  std::vector<std::string> vector;
  for (int i = 0; i < length; i++) {
    auto string = (jstring) (env->GetObjectArrayElement(str_array, i));
    vector.push_back(JStringToCString(env, string));
  }
  return vector;
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeDataSource
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeDataSource
    (JNIEnv *, jobject, jlong);

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    getFragments
 * Signature: (J[Ljava/lang/String;[B)[J
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_getFragments
    (JNIEnv *, jobject, jlong, jobjectArray, jbyteArray);

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    scan
 * Signature: (J)[J
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_scan
    (JNIEnv *, jobject, jlong);

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeFragment
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeFragment
    (JNIEnv *, jobject, jlong);

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    nextRecordBatch
 * Signature: (J)Lorg/apache/arrow/dataset/jni/NativeRecordBatchHandle;
 */
JNIEXPORT jobject JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_nextRecordBatch
    (JNIEnv *, jobject, jlong);

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeScanTask
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeScanTask
    (JNIEnv *, jobject, jlong);

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    releaseBuffer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_releaseBuffer
    (JNIEnv *, jobject, jlong);

/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    makeSource
 * Signature: ([Ljava/lang/String;II)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_file_JniWrapper_makeSource
        (JNIEnv *, jobject, jobjectArray, jint, jint);

/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    getSchema
 * Signature: ([Ljava/lang/String;II)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_arrow_dataset_file_JniWrapper_getSchema
        (JNIEnv *env, jobject, jobjectArray paths, jint file_format_id, jint file_system_id) {
  std::shared_ptr<arrow::dataset::FileFormat> file_format = GetFileFormat(env, file_format_id);
  std::shared_ptr<arrow::fs::FileSystem> fs = GetFileSystem(env, file_system_id);
  std::vector<std::string> path_vector = ToStringVector(env, paths);
  std::vector<std::shared_ptr<arrow::dataset::FileSource>> file_srcs;
  for (const auto& path : path_vector) {
    std::shared_ptr<arrow::dataset::FileSource> file_src = std::make_shared<arrow::dataset::FileSource>(path, fs.get());
    file_srcs.push_back(std::move(file_src));
  }
  std::shared_ptr<arrow::Schema> schema = file_format->Inspect(*file_srcs.at(0)).ValueOrDie();

  std::shared_ptr<arrow::Buffer> out;

  auto status =
      arrow::ipc::SerializeSchema(*schema, nullptr, arrow::default_memory_pool(), &out);
  if (!status.ok()) {
    std::string error_message = "unable to serialize schema";
    env->ThrowNew(runtime_exception_class, error_message.c_str());
  }

  jbyteArray ret = env->NewByteArray(out->size());
  auto src = reinterpret_cast<const jbyte*>(out->data());
  env->SetByteArrayRegion(ret, 0, out->size(), src);
  return ret;
}