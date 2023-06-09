#include <jni.h>
/* Header for class JniExample */

#ifndef _Included_JniExample
#define _Included_JniExample
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     JniExample
 * Method:    helloworld
 * Signature: ()V
 */
JNIEXPORT int64_t JNICALL Java_JniExample_helloworld(JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif
#endif
