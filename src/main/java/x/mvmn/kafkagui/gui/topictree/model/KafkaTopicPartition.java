package x.mvmn.kafkagui.gui.topictree.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KafkaTopicPartition implements Comparable<KafkaTopicPartition> {

	private String topic;
	private Integer number;

	@Override
	public int compareTo(KafkaTopicPartition o) {
		int num = number != null ? number.intValue() : -1;
		Integer otherNumber = o.getNumber();
		int otherNum = otherNumber != null ? otherNumber.intValue() : -1;
		return num - otherNum;
	}

	public String toString() {
		return number != null ? number.toString() : "null";
	}
}
