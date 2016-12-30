/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class utils_external_NativeRoutines */

#ifndef _Included_utils_external_NativeRoutines
#define _Included_utils_external_NativeRoutines
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     utils_external_NativeRoutines
 * Method:    poolAndRectify
 * Signature: (IIIIIDD[D)[D
 */
JNIEXPORT jdoubleArray JNICALL Java_utils_external_NativeRoutines_poolAndRectify
  (JNIEnv *, jobject, jint, jint, jint, jint, jint, jdouble, jdouble, jdoubleArray);

/*
 * Class:     utils_external_NativeRoutines
 * Method:    fwht
 * Signature: ([DI)[D
 */
JNIEXPORT jdoubleArray JNICALL Java_utils_external_NativeRoutines_fwht
  (JNIEnv *, jobject, jdoubleArray, jint);

/*
 * Class:     utils_external_NativeRoutines
 * Method:    fastfood
 * Signature: ([D[D[D[D[DIIII)[D
 */
JNIEXPORT jdoubleArray JNICALL Java_utils_external_NativeRoutines_fastfood
  (JNIEnv *, jobject, jdoubleArray, jdoubleArray, jdoubleArray, jdoubleArray, jdoubleArray, jint, jint, jint, jint);

/*
 * Class:     utils_external_NativeRoutines
 * Method:    cosine
 * Signature: (F)F
 */
JNIEXPORT jfloat JNICALL Java_utils_external_NativeRoutines_cosine
  (JNIEnv *, jobject, jfloat);

/*
 * Class:     utils_external_NativeRoutines
 * Method:    NewDualLeastSquaresEstimator
 * Signature: (IIID)J
 */
JNIEXPORT jlong JNICALL Java_utils_external_NativeRoutines_NewDualLeastSquaresEstimator
  (JNIEnv *, jobject, jint, jint, jint, jdouble);

/*
 * Class:     utils_external_NativeRoutines
 * Method:    DeleteDualLeastSquaresEstimator
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_utils_external_NativeRoutines_DeleteDualLeastSquaresEstimator
  (JNIEnv *, jobject, jlong);

/*
 * Class:     utils_external_NativeRoutines
 * Method:    DualLeastSquaresEstimatorAccumulateGram
 * Signature: (J[D)V
 */
JNIEXPORT void JNICALL Java_utils_external_NativeRoutines_DualLeastSquaresEstimatorAccumulateGram
  (JNIEnv *, jobject, jlong, jdoubleArray);

/*
 * Class:     utils_external_NativeRoutines
 * Method:    DualLeastSquaresEstimatorSolve
 * Signature: (J[D)[D
 */
JNIEXPORT jdoubleArray JNICALL Java_utils_external_NativeRoutines_DualLeastSquaresEstimatorSolve
  (JNIEnv *, jobject, jlong, jdoubleArray);

#ifdef __cplusplus
}
#endif
#endif
