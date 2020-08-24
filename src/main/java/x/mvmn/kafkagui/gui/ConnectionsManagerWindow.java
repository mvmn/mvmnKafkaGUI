package x.mvmn.kafkagui.gui;

import java.awt.BorderLayout;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.SortedSet;

import javax.swing.AbstractListModel;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;

import x.mvmn.kafkagui.gui.util.SwingUtil;
import x.mvmn.kafkagui.model.KafkaConfigModel;

public class ConnectionsManagerWindow extends JFrame {
	private static final long serialVersionUID = -3884034416111932108L;

	private final File appHomeFolder;

	private final JList<String> configsList;
	private final JPanel configPanel = new JPanel(new BorderLayout());
	private volatile KafkaConfigPanel currentKafkaConfig = new KafkaConfigPanel();
	private volatile int currentlySelectedConfig = 0;
	private final List<String> configs = Collections.synchronizedList(new ArrayList<>());
	private final JButton btnSave = new JButton("Save");
	private final JButton btnDelete = new JButton("Delete");

	private final JButton btnTestConnection = new JButton("Test connection");
	private final JButton btnConnect = new JButton("Connect");
	private final ConfigsListModel configListModel = new ConfigsListModel();

	public ConnectionsManagerWindow(File appHomeFolder, SortedSet<String> existingConnectionConfigs) {
		super("MVMn Kafka Client GUI");
		this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		this.appHomeFolder = appHomeFolder;

		configs.add("New connection...");
		for (String configName : existingConnectionConfigs) {
			configs.add(configName);
		}

		configsList = new JList<>(configListModel);
		configsList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		configsList.setSelectedIndex(0);
		currentKafkaConfig.registerDirtyListener(this::configDirty);

		btnDelete.setEnabled(false);
		configsList.addListSelectionListener(lse -> {
			if (currentKafkaConfig.isDirty()) {
				if (JOptionPane.YES_OPTION == JOptionPane.showConfirmDialog(ConnectionsManagerWindow.this, "Config changed - save changes?",
						"Config changed", JOptionPane.YES_NO_OPTION)) {
					btnSave.doClick();
				}
			}
			int idx = configsList.getSelectedIndex();
			currentlySelectedConfig = idx;

			btnDelete.setEnabled(idx > 0);
			if (idx > 0) {
				String configName = configs.get(idx);
				SwingUtil.performSafely(() -> {
					try (FileInputStream fis = new FileInputStream(new File(appHomeFolder, configName + ".properties"))) {
						KafkaConfigModel model = new KafkaConfigModel();
						Properties props = new Properties();
						props.load(fis);
						model.setModelFromProperties(props);
						SwingUtilities.invokeLater(() -> {
							setConfig(model);
						});
					}
				});
			} else {
				setConfig(new KafkaConfigModel());
			}
		});

		this.setLayout(new BorderLayout());
		configPanel.add(SwingUtil.twoComponentPanel(btnSave, btnDelete), BorderLayout.NORTH);
		configPanel.add(SwingUtil.twoComponentPanel(btnTestConnection, btnConnect), BorderLayout.SOUTH);
		configPanel.add(new JScrollPane(currentKafkaConfig), BorderLayout.CENTER);

		JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, true, new JScrollPane(configsList), configPanel);
		splitPane.setResizeWeight(0.2);
		this.add(splitPane);

		btnDelete.addActionListener(actEvt -> {
			int idx = configsList.getSelectedIndex();
			if (idx > 0) {
				String configName = configs.get(idx);
				if (JOptionPane.YES_OPTION == JOptionPane.showConfirmDialog(ConnectionsManagerWindow.this,
						"Are you sure you want to delete config " + configName + "?", "Delete config", JOptionPane.YES_NO_OPTION)) {
					SwingUtil.performSafely(() -> {
						new File(appHomeFolder, configName + ".properties").delete();
						SwingUtilities.invokeLater(() -> removeConfigFromList(idx));
					});
				}
			}
		});
		btnSave.addActionListener(actEvt -> {
			btnSave.setEnabled(false);

			boolean newConfig = currentlySelectedConfig < 1;
			String configName = null;
			if (newConfig) {
				configName = JOptionPane.showInputDialog(ConnectionsManagerWindow.this, "Enter config file name");
			} else {
				configName = configs.get(currentlySelectedConfig);
			}

			if (configName != null) {
				String normalizedConfigName = Normalizer.normalize(configName, Normalizer.Form.NFD).replaceAll("[^A-Za-z0-9 _\\-\\.]", "_");
				KafkaConfigModel configModel = currentKafkaConfig.getCurrentState();
				SwingUtil.performSafely(() -> {
					String fileName = normalizedConfigName + ".properties";
					try (FileOutputStream fos = new FileOutputStream(new File(ConnectionsManagerWindow.this.appHomeFolder, fileName))) {
						Properties props = configModel.modelToProperties();
						props.store(fos, "MVMn Kafka Client GUI");
						currentKafkaConfig.setNotDirty();
						if (newConfig) {
							SwingUtilities.invokeLater(() -> addConfigToList(fileName));
						}
					} finally {
						btnSave.setEnabled(true);
					}
				});
			} else {
				btnSave.setEnabled(true);
			}
		});
	}

	protected void setConfig(KafkaConfigModel configModel) {
		configPanel.removeAll();
		currentKafkaConfig = new KafkaConfigPanel(configModel);
		currentKafkaConfig.registerDirtyListener(this::configDirty);
		configPanel.add(SwingUtil.twoComponentPanel(btnSave, btnDelete), BorderLayout.NORTH);
		configPanel.add(SwingUtil.twoComponentPanel(btnTestConnection, btnConnect), BorderLayout.SOUTH);
		configPanel.add(new JScrollPane(currentKafkaConfig), BorderLayout.CENTER);
		configPanel.invalidate();
		configPanel.revalidate();
		configPanel.repaint();
	}

	protected class ConfigsListModel extends AbstractListModel<String> {
		private static final long serialVersionUID = -8820583687358458851L;

		@Override
		public int getSize() {
			return configs.size();
		}

		@Override
		public String getElementAt(int index) {
			if (index >= configs.size()) {
				return "";
			}
			String configName = configs.get(index);
			boolean dirty = index == currentlySelectedConfig && currentKafkaConfig.isDirty();
			return configName + (dirty ? " *" : "");
		}

		public void onConfigAdded() {
			this.fireIntervalAdded(this, configs.size(), configs.size());
		}

		public void onConfigDeleted(int index) {
			this.fireIntervalRemoved(this, index, index);
		}

		public void onConfigDirty() {
			int idx = configsList.getSelectedIndex();
			this.fireContentsChanged(this, idx, idx);
		}
	}

	protected void configDirty() {
		configListModel.onConfigDirty();
	}

	protected void removeConfigFromList(int index) {
		configs.remove(index);
		configsList.setSelectedIndex(0);
		currentlySelectedConfig = 0;
		configListModel.onConfigDeleted(index);
	}

	protected void addConfigToList(String fileName) {
		if (fileName != null && fileName.endsWith(".properties")) {
			String configName = fileName.substring(0, fileName.length() - ".properties".length());
			configs.add(configName);

			configListModel.onConfigAdded();
			configsList.setSelectedIndex(configs.size() - 1);
			currentlySelectedConfig = configs.size() - 1;
			configsList.invalidate();
			configsList.revalidate();
			configsList.repaint();
		}
	}
}
