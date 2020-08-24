package x.mvmn.kafkagui.gui.topictree.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KafkaTopic implements Comparable<KafkaTopic> {

	private String name;
	private boolean internal;

	public String toString() {
		return name + (internal ? " *" : "");
	}

	@Override
	public int compareTo(KafkaTopic o) {
		if (this == o) {
			return 0;
		}
		if (o == null) {
			return -1;
		}
		return this.getName().compareTo(o.getName());
	}
}
