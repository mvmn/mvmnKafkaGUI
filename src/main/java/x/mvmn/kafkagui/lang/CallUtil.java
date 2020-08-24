package x.mvmn.kafkagui.lang;

import java.util.function.Consumer;

public class CallUtil {

	public static void doUnsafe(UnsafeOperation task) {
		try {
			task.run();
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}
		}
	}

	public static Runnable unsafe(UnsafeOperation task) {
		return () -> doUnsafe(task);
	}

	public static <T> Consumer<T> unsafe(UnsafeConsumer<T> task) {
		return v -> doUnsafe(() -> task.accept(v));
	}
}
