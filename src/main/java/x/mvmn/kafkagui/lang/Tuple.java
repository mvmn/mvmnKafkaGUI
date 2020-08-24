package x.mvmn.kafkagui.lang;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Tuple<A, B, C, D, E> {
	protected A a;
	protected B b;
	protected C c;
	protected D d;
	protected E e;
}
