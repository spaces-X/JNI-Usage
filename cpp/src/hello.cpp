#include "JniExample.h"
#include <iostream>

JNIEXPORT int64_t JNICALL Java_JniExample_helloworld(JNIEnv *, jobject) {
    std::cout<<"hello from cpp" << std::endl;
    return 0x7fff;
}

