package x.mvmn.kafkagui.gui;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;

public class KafkaConfigPanel extends JPanel {
	private static final long serialVersionUID = 5540440699197585775L;

	private final Map<String, JTextField> singeValProps = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, List<JTextField>> multiValProps = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, JPanel> multiValPropPanels = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, ConfigKey> configKeysByName;

	public KafkaConfigPanel() {
		this(null);
	}

	public KafkaConfigPanel(KafkaConfigModel initialState) {
		super(new GridBagLayout());
		KafkaConfigModel model = initialState != null ? initialState : new KafkaConfigModel();
		Map<String, ConfigKey> configKeysByName = new HashMap<>();

		GridBagConstraints gbc = new GridBagConstraints();
		List<ConfigKey> configKeys = model.getConfigKeys();
		for (int i = 0; i < configKeys.size(); i++) {
			ConfigKey configKey = configKeys.get(i);
			String key = configKey.name;

			configKeysByName.put(key, configKey);

			JComponent component;
			if (configKey.type.equals(ConfigDef.Type.LIST)) {
				List<String> values = model.getListPropety(configKey);
				List<JTextField> inputs = new ArrayList<>();
				if (values != null && !values.isEmpty()) {
					for (String value : values) {
						inputs.add(new JTextField(value));
					}
				}
				JPanel panel = new JPanel();
				component = panel;
				multiValPropPanels.put(key, panel);
				multiValProps.put(key, inputs);
				repopulatePanel(key);
			} else if (configKey.type.equals(ConfigDef.Type.PASSWORD)) {
				JTextField txf = new JPasswordField(model.getProperty(configKey));
				component = txf;
				singeValProps.put(key, txf);
			} else {
				JTextField txf = new JTextField(model.getProperty(configKey));
				component = txf;
				singeValProps.put(key, txf);
			}
			gbc.gridy = i;
			// gbc.gridx = 0;
			// gbc.weightx = 0.0;
			// gbc.fill = GridBagConstraints.BOTH;
			// JLabel label = new JLabel(key, JLabel.RIGHT);
			// label.setVerticalAlignment(JLabel.TOP);
			// this.add(label, gbc);
			component.setBorder(BorderFactory.createTitledBorder(key));
			gbc.fill = GridBagConstraints.HORIZONTAL;
			gbc.weightx = 1.0;
			gbc.gridx = 0;
			this.add(component, gbc);
		}

		this.configKeysByName = Collections.unmodifiableMap(configKeysByName);
	}

	private void repopulatePanel(String key) {
		List<JTextField> panelInputs = multiValProps.get(key);
		JPanel panel = multiValPropPanels.get(key);
		panel.removeAll();
		panel.setLayout(new GridBagLayout());
		JButton addBtn = new JButton("Add");
		addBtn.addActionListener(e -> {
			multiValProps.get(key).add(new JTextField());
			repopulatePanel(key);
			panel.invalidate();
			panel.revalidate();
			panel.repaint();
		});
		GridBagConstraints gbc = new GridBagConstraints();
		gbc.fill = GridBagConstraints.HORIZONTAL;
		for (int i = 0; i < panelInputs.size(); i++) {
			JTextField input = panelInputs.get(i);
			final JTextField currentInput = input;
			gbc.gridy = i;
			gbc.gridx = 0;
			gbc.weightx = 1.0;
			panel.add(input, gbc);
			JButton deleteBtn = new JButton("x");
			gbc.weightx = 0.0;
			gbc.gridx = 1;
			panel.add(deleteBtn, gbc);
			deleteBtn.addActionListener(e -> {
				multiValProps.get(key).remove(currentInput);
				repopulatePanel(key);
				panel.invalidate();
				panel.revalidate();
				panel.repaint();
			});
		}
		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.weightx = 1.0;
		gbc.gridy = panelInputs.size();
		gbc.gridx = 0;
		gbc.gridwidth = 2;
		panel.add(addBtn, gbc);
	}

	public KafkaConfigModel getCurrentState() {
		KafkaConfigModel result = new KafkaConfigModel();

		for (Map.Entry<String, JTextField> svp : singeValProps.entrySet()) {
			String value = svp.getValue() instanceof JPasswordField ? new String(((JPasswordField) svp.getValue()).getPassword())
					: svp.getValue().getText();
			result.setProperty(configKeysByName.get(svp.getKey()), value.trim().isEmpty() ? null : value.trim());
		}
		for (Map.Entry<String, List<JTextField>> mvp : multiValProps.entrySet()) {
			List<String> values = mvp.getValue().stream().map(JTextField::getText).filter(v -> v != null && !v.trim().isEmpty())
					.collect(Collectors.toList());
			result.setListProperty(configKeysByName.get(mvp.getKey()), values);
		}

		return result;
	}
}
