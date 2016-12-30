 /** @internal
 ** @file     FisherExtractor.cxx
 ** @brief    JNI Wrapper for enceval GMM and Fisher Vector
 **/

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <ctype.h>
#include <math.h>
#include <string.h>
#include <ctime>
#include <stdexcept>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <iostream>
#include <Eigen/Dense>
#include <Eigen/Sparse>
#include <stdlib.h>
#include <iostream>


#include "fht_header_only.h"
#include "NativeRoutines.h"


using namespace Eigen;
using namespace std;


static inline jint imageToVectorCoords(jint x, jint y, jint c, jint yDim, jint xDim) {
  return y + x * yDim + c * yDim * xDim;
}

JNIEXPORT jdoubleArray JNICALL Java_utils_external_NativeRoutines_fastfood(
    JNIEnv* env,
    jobject obj,
    jdoubleArray gaussian,
    jdoubleArray radamacher,
    jdoubleArray uniform,
    jdoubleArray chiSquared,
    jdoubleArray patchMatrix,
    jint seed,
    jint outSize,
    jint inSize,
    jint numPatches)
{
  double* out;

  int ret = posix_memalign((void**) &out, 32, outSize*numPatches*sizeof(double));
  if (ret != 0) {
    throw std::runtime_error("Ran out of memory!");
  }

  jdouble* patchMatrixV = env->GetDoubleArrayElements(patchMatrix, 0);
  jdouble* radamacherV = env->GetDoubleArrayElements(radamacher, 0);
  jdouble* uniformV= env->GetDoubleArrayElements(uniform, 0);
  jdouble* gaussianV = env->GetDoubleArrayElements(gaussian, 0);
  jdouble* chiSquaredV = env->GetDoubleArrayElements(chiSquared, 0);

  /* (outSize x numPatches) matrix */
  Map< Array<double, Dynamic, Dynamic> > outM(out, outSize, numPatches);
  Map< Array<double, Dynamic, Dynamic> > mf(patchMatrixV, inSize, numPatches);
  Map< Array<double, Dynamic, 1> > radamacherVector(radamacherV, outSize);
  Map< Array<double, Dynamic, 1> > uniformVector(uniformV, outSize);
  Map< Array<double, Dynamic, 1> > gaussianVector(gaussianV, outSize);
  Map< Array<double, Dynamic, 1> > chisquaredVector(chiSquaredV, outSize);
  for (int i = 0; i < outSize; i += inSize) {
    outM.block(i, 0, inSize, numPatches) = mf;
    outM.block(i, 0, inSize, numPatches).colwise() *=  radamacherVector.segment(i, inSize);
    for (int j = 0; j < numPatches; j += 1) {
      double* patch = out + (j*outSize) + i;
      FHTDouble(patch, inSize, 2048);
    }
    outM.block(i, 0, inSize, numPatches).colwise() *= gaussianVector.segment(i, inSize);
    for (int j = 0; j < numPatches; j += 1) {
      double* patch = out + (j*outSize) + i;
      FHTDouble(patch, inSize, 2048);
    }
    outM.block(i, 0, inSize, numPatches).colwise() *= chisquaredVector.segment(i, inSize);
  }

  jdoubleArray result = env->NewDoubleArray(outSize*numPatches);
  env->SetDoubleArrayRegion(result, 0, outSize*numPatches, out);
  free(out);
  env->ReleaseDoubleArrayElements(patchMatrix, patchMatrixV, JNI_ABORT);
  env->ReleaseDoubleArrayElements(radamacher, radamacherV, JNI_ABORT);
  env->ReleaseDoubleArrayElements(uniform, uniformV, JNI_ABORT);
  env->ReleaseDoubleArrayElements(gaussian, gaussianV, JNI_ABORT);
  env->ReleaseDoubleArrayElements(chiSquared, chiSquaredV, JNI_ABORT);
  return result;
}

