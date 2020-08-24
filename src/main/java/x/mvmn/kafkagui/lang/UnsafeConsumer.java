package x.mvmn.kafkagui.lang;

public interface UnsafeConsumer<T> {

	void accept(T t) throws Exception;

}
