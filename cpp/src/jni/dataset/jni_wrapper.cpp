//
// Created by root on 11/26/19.
//

#include "jni/concurrent_map.h"

#include "org_apache_arrow_dataset_file_JniWrapper.h"
#include "org_apache_arrow_dataset_jni_JniWrapper.h"


using arrow::jni::ConcurrentMap;

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
        (JNIEnv *, jobject, jobjectArray, jint, jint);
