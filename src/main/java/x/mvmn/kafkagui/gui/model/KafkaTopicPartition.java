package x.mvmn.kafkagui.gui.model;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
@Builder
public class KafkaTopicPartition implements Comparable<KafkaTopicPartition> {

	private final String topic;
	private final Integer number;

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
