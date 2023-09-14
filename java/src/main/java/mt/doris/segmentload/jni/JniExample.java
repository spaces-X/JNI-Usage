package mt.doris.segmentload.jni;

public class JniExample {

    static {
        String libraryPath = System.getProperty("java.library.path");
        System.out.println(libraryPath);
        System.loadLibrary("segment_builder_tool");
    }

    public native void segmentBuild(String metaFilePath, String dataDirPath);

//    public static void main(String[] args) {
//
//        if (args.length != 2) {
//            System.out.println("please input the metaFilePath and dataDirPath");
//        }
//        String metaFilePath = args[0];
//        String dataDirPath = args[1];
//       org.apache.doris.mt.doris.segmentload.jni.JniExample example = new org.apache.doris.mt.doris.segmentload.jni.JniExample();
//       example.segmentBuild(metaFilePath, dataDirPath);
//    }
}
