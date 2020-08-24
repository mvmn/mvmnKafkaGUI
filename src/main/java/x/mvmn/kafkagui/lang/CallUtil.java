package x.mvmn.kafkagui.lang;

public class CallUtil {

	public static void doSafely(UnsafeOperation task) {
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
}
