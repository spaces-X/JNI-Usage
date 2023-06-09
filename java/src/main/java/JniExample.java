public class JniExample {

    static {
        System.loadLibrary("hello");
    }
    public native long helloworld();

    public static void main(String[] args) {
       JniExample example = new JniExample();
       long c = example.helloworld();
        System.out.println(Long.toHexString(c));
    }
}
