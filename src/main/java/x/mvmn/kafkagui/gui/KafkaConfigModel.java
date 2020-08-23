package x.mvmn.kafkagui.gui;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;

public class KafkaConfigModel {
	private final Map<String, List<String>> listsModel = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, String> model = Collections.synchronizedMap(new HashMap<>());
	private final List<ConfigKey> configKeys;

	public KafkaConfigModel() {
		List<ConfigKey> configKeys = new ArrayList<>();
		for (Map.Entry<String, ConfigKey> configKeyEntry : AdminClientConfig.configDef().configKeys().entrySet()) {
			String key = configKeyEntry.getKey();
			ConfigKey configKey = configKeyEntry.getValue();
			configKeys.add(configKey);
			if (configKey != null && configKey.defaultValue != null) {
				switch (configKey.type) {
					case LIST:
						List<String> values = new ArrayList<>();
						if (configKey.defaultValue instanceof List) {
							((List<?>) configKey.defaultValue).stream().map(Object::toString).map(Object::toString).forEach(values::add);
						}
						listsModel.put(key, values);
					break;
					default:
						model.put(key, configKey.defaultValue.toString());
					break;
				}
			}
			if (key.equals("client.id")) {
				model.put(key, "mvmn_kafkaclient_gui");
			} else if (key.equals("bootstrap.servers") && listsModel.get(key).isEmpty()) {
				listsModel.get(key).add("127.0.0.1:9092");
			}
		}
		this.configKeys = Collections.unmodifiableList(configKeys);
	}

	public Properties modelToProperties() {
		Properties props = new Properties();
		for (Map.Entry<String, String> singleValues : model.entrySet()) {
			if (singleValues.getValue() != null) {
				props.setProperty(singleValues.getKey(), singleValues.getValue());
			}
		}
		for (Map.Entry<String, List<String>> listValues : listsModel.entrySet()) {
			if (listValues.getValue() != null && !listValues.getValue().isEmpty()
					&& !(listValues.getValue().size() == 1 && listValues.getValue().get(0).isEmpty())) {
				props.setProperty(listValues.getKey(), listValues.getValue().stream().collect(Collectors.joining(",")));
			}
		}
		return props;
	}

	public void setModelFromProperties(Properties props) {
		for (ConfigKey configKey : configKeys) {
			String value = props.getProperty(configKey.name);
			if (value != null && !value.isEmpty()) {
				if (configKey.type.equals(ConfigDef.Type.LIST)) {
					listsModel.put(configKey.name, new ArrayList<>(Arrays.asList(value.split(","))));
				} else {
					model.put(configKey.name, value);
				}
			} else {
				if (configKey.type.equals(ConfigDef.Type.LIST)) {
					listsModel.remove(configKey.name);
				} else {
					model.remove(configKey.name);
				}
			}
		}
	}

	public List<ConfigKey> getConfigKeys() {
		return configKeys;
	}

	public String getProperty(ConfigKey configKey) {
		if (configKey.type.equals(ConfigDef.Type.LIST)) {
			return Optional.ofNullable(listsModel.get(configKey.name)).orElseGet(Collections::emptyList).stream()
					.collect(Collectors.joining(","));
		} else {
			return model.get(configKey.name);
		}
	}

	public KafkaConfigModel setProperty(ConfigKey configKey, String value) {
		if (configKey.type.equals(ConfigDef.Type.LIST)) {
			setListProperty(configKey, value != null ? Arrays.asList(value.split(",")) : null);
		} else {
			this.model.put(configKey.name, value);
		}
		return this;
	}

	public List<String> getListPropety(ConfigKey configKey) {
		return listsModel.get(configKey.name);
	}

	public KafkaConfigModel setListProperty(ConfigKey configKey, List<String> value) {
		listsModel.put(configKey.name, value != null ? new ArrayList<>(value) : null);
		return this;
	}
}
