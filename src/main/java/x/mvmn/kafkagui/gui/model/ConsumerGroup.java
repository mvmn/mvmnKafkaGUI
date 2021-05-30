package x.mvmn.kafkagui.gui.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConsumerGroup implements Comparable<ConsumerGroup> {

	private String consumerGroupId;
	private boolean special;

	public String toString() {
		return consumerGroupId + (special ? " *" : "");
	}

	@Override
	public int compareTo(ConsumerGroup o) {
		if (this == o) {
			return 0;
		}
		if (o == null) {
			return -1;
		}
		return this.getConsumerGroupId().compareTo(o.getConsumerGroupId());
	}
}
