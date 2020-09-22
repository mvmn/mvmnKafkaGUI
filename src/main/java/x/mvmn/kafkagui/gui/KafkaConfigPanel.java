package x.mvmn.kafkagui.gui;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;

import x.mvmn.kafkagui.model.KafkaConfigModel;

public class KafkaConfigPanel extends JPanel {
	private static final long serialVersionUID = 5540440699197585775L;

	private final Map<String, JTextField> singeValProps = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, List<JTextField>> multiValProps = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, JPanel> multiValPropPanels = Collections.synchronizedMap(new HashMap<>());
	private final Map<String, ConfigKey> configKeysByName;
	private volatile boolean dirty = false;
	private final DocumentListener dirtyListener = new DocumentListener() {

		@Override
		public void removeUpdate(DocumentEvent e) {
			setDirty();
		}

		@Override
		public void insertUpdate(DocumentEvent e) {
			setDirty();
		}

		@Override
		public void changedUpdate(DocumentEvent e) {
			setDirty();
		}
	};
	private final CopyOnWriteArrayList<Runnable> dirtyListeners = new CopyOnWriteArrayList<>();

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
						inputs.add(regDirtyListener(new JTextField(value)));
					}
				}
				JPanel panel = new JPanel();
				component = panel;
				multiValPropPanels.put(key, panel);
				multiValProps.put(key, inputs);
				repopulatePanel(key);
			} else if (configKey.type.equals(ConfigDef.Type.PASSWORD)) {
				JTextField txf = regDirtyListener(new JPasswordField(model.getProperty(configKey)));
				component = txf;
				singeValProps.put(key, txf);
			} else {
				JTextField txf = regDirtyListener(new JTextField(model.getProperty(configKey)));
				component = txf;
				singeValProps.put(key, txf);
			}
			gbc.gridy = i;
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
			setDirty();
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
				setDirty();
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

	protected <T extends JTextField> T regDirtyListener(T component) {
		component.getDocument().addDocumentListener(dirtyListener);
		return component;
	}

	public boolean isDirty() {
		return dirty;
	}

	public void setDirty() {
		this.dirty = true;
		notifyListeners();
	}

	public void setNotDirty() {
		this.dirty = false;
		notifyListeners();
	}

	protected void notifyListeners() {
		Runnable[] listeners = dirtyListeners.toArray(new Runnable[0]);
		for (Runnable listener : listeners) {
			listener.run();
		}
	}

	public void registerDirtyListener(Runnable dirtyListener) {
		this.dirtyListeners.add(dirtyListener);
	}

	public void deregisterDirtyListeners() {
		this.dirtyListeners.clear();
	}

}
