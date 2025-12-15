package factor;

/**
 * 本地占位入口。
 *
 * <p>真实的 Hadoop MapReduce 入口是 {@link Driver}。该类仅用于在不启动 Hadoop 的情况下快速验证
 * 工程可编译/可运行，避免 IDE 误判为“没有 main”。</p>
 */
public final class App {
    private App() {
        // Prevent instantiation.
    }

    public static void main(String[] args) {
        System.out.println("Factor MapReduce project skeleton ready.");
    }
}
