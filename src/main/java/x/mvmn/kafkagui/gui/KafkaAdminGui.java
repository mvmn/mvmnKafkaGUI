package x.mvmn.kafkagui.gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.swing.BorderFactory;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.ListSelectionModel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.table.DefaultTableModel;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreeSelectionModel;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import groovy.util.Eval;
import x.mvmn.kafkagui.gui.topictree.model.KafkaTopic;
import x.mvmn.kafkagui.gui.topictree.model.KafkaTopicPartition;
import x.mvmn.kafkagui.gui.util.SwingUtil;
import x.mvmn.kafkagui.lang.HexUtil;
import x.mvmn.kafkagui.lang.StackTraceUtil;
import x.mvmn.kafkagui.lang.Tuple;

public class KafkaAdminGui extends JFrame {
	private static final long serialVersionUID = 3826007764248597964L;

	protected final DefaultMutableTreeNode topicsRootNode = new DefaultMutableTreeNode("Topics", true);
	protected final DefaultTreeModel treeModel = new DefaultTreeModel(topicsRootNode);
	protected final JTree topicsTree = new JTree(treeModel);
	protected final JButton btnCreateTopic = new JButton("Create");
	protected final JButton btnDeleteTopic = new JButton("Delete");
	protected final JPanel contentPanel = new JPanel(new BorderLayout());
	protected final DefaultTableModel msgTableModel = new DefaultTableModel(new String[] { "Partition", "Offset", "Key", "Content" }, 0) {
		private static final long serialVersionUID = -4104977444040382766L;

		@Override
		public boolean isCellEditable(int row, int column) {
			return false;
		}
	};
	protected final JTable msgTable = new JTable(msgTableModel);
	protected final JPanel msgPanel = new JPanel(new GridBagLayout());
	protected final JPanel topicMessagesPanel = new JPanel(new GridBagLayout());
	protected final JTextField msgOffsetField = new JTextField();
	protected final JTextField msgKeyField = new JTextField();
	protected final JTextArea msgContent = new JTextArea();
	protected final JComboBox<String> msgViewEncoding = new JComboBox<>(
			new DefaultComboBoxModel<>(Charset.availableCharsets().keySet().toArray(new String[0])));
	protected final JCheckBox msgViewHex = new JCheckBox("Hex");

	protected final JComboBox<String> msgGetOption = new JComboBox<>(new String[] { "Latest", "Earliest" });
	protected final JTextField msgGetCount = SwingUtil.numericOnlyTextField(10L, 0L, null, false);
	protected final JTextField msgReadTopic = new JTextField("");
	protected final JTextField msgReadPartition = new JTextField("");
	protected final JTextField msgDetectedEndOffset = new JTextField("");
	protected final JTextField msgDetectedBeginOffset = new JTextField("");
	protected final JButton btnGetMessages = new JButton("Get messages");
	protected final JButton btnPostMessage = new JButton("Post message");
	protected volatile AdminClient kafkaAdminClient;
	protected final Properties clientConfig;
	protected final List<ConsumerRecord<String, byte[]>> currentResults = new CopyOnWriteArrayList<>();

	protected final JComboBox<String> msgPostProcessor = new JComboBox<>(new String[] { "None", "JSON pretty-print", "Groovy script" });
	protected final JTextArea txaGroovyTransform = new JTextArea(DEFAULT_GROOVY_TRANSFORMER_CODE);

	protected final Font defaultFont;
	protected final Font monospacedFont;
	protected final ObjectMapper objectMapper = new ObjectMapper();

	private static final String DEFAULT_GROOVY_TRANSFORMER_CODE = "if(content.length>0 && (content[0] == '{' || content[0] == '[')) {\n"
			+ "    om = new com.fasterxml.jackson.databind.ObjectMapper(); \n"
			+ "    return om.writerWithDefaultPrettyPrinter().writeValueAsBytes(om.readTree(content));\n}\nreturn content;";

	protected volatile boolean receiveInProgress = false;
	protected volatile boolean topicOrPartitionSelected = false;

	public KafkaAdminGui(String configName, Properties clientConfig, File appHomeFolder) {
		super(configName + " - MVMn Kafka Client GUI");
		this.clientConfig = clientConfig;
		this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		this.setLayout(new BorderLayout());
		JLabel label = new JLabel("Connecting...", SwingConstants.CENTER);

		this.defaultFont = label.getFont();
		this.monospacedFont = new Font(Font.MONOSPACED, defaultFont.getStyle(), defaultFont.getSize());

		label.setBorder(BorderFactory.createEmptyBorder(32, 32, 32, 32));
		this.add(label, BorderLayout.CENTER);
		this.pack();
		SwingUtil.moveToScreenCenter(this);

		this.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent e) {
				String groovyTransformContent = txaGroovyTransform.getText();
				new Thread() {
					public void run() {
						try {
							File groovyTransformerCode = new File(appHomeFolder, "groovyTransformer.groovy");
							FileUtils.write(groovyTransformerCode, groovyTransformContent, StandardCharsets.UTF_8, false);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}.start();
			}

			@Override
			public void windowClosed(WindowEvent e) {
				AdminClient ac = kafkaAdminClient;
				if (ac != null) {
					SwingUtil.performSafely(() -> ac.close());
				}
			}
		});

		this.setVisible(true);

		SwingUtil.performSafely(() -> {
			File groovyTransformerCode = new File(appHomeFolder, "groovyTransformer.groovy");
			if (groovyTransformerCode.exists() && groovyTransformerCode.isFile()) {
				String content = FileUtils.readFileToString(groovyTransformerCode, StandardCharsets.UTF_8);
				SwingUtilities.invokeLater(() -> txaGroovyTransform.setText(content));
			}
		});

		SwingUtil.performSafely(() -> {
			AdminClient ac = KafkaAdminClient.create(clientConfig);
			this.kafkaAdminClient = ac;
			// Perform list topics as a test
			Collection<KafkaTopic> topics = ac.listTopics(new ListTopicsOptions().listInternal(true)).listings().get().stream()
					.map(topic -> new KafkaTopic(topic.name(), topic.isInternal())).sorted().collect(Collectors.toList());

			Map<String, TopicDescription> topicDescriptions = ac
					.describeTopics(topics.stream().map(KafkaTopic::getName).collect(Collectors.toSet()),
							new DescribeTopicsOptions().includeAuthorizedOperations(true))
					.all().get();

			topicsTree.getSelectionModel().addTreeSelectionListener(e -> onTopicsTreeSelectionChange());

			SwingUtilities.invokeLater(() -> {
				KafkaAdminGui.this.setVisible(false);
				for (KafkaTopic topic : topics) {
					topicsRootNode.add(createTopicNode(topic, topicDescriptions.get(topic.getName())));
				}
				topicsTree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
				topicsTree.expandRow(0);
				KafkaAdminGui.this.remove(label);
				JPanel topicsPanel = new JPanel(new BorderLayout());
				topicsPanel.add(new JScrollPane(topicsTree), BorderLayout.CENTER);
				topicsPanel.add(SwingUtil.twoComponentPanel(btnCreateTopic, btnDeleteTopic), BorderLayout.SOUTH);
				JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, true, topicsPanel, contentPanel);
				splitPane.setResizeWeight(0.2);

				msgViewEncoding.setSelectedItem(StandardCharsets.UTF_8.name());
				GridBagConstraints gbc = new GridBagConstraints();
				gbc.fill = GridBagConstraints.HORIZONTAL;
				gbc.weighty = 0.0;
				gbc.gridy = 0;
				gbc.gridx = 0;
				gbc.weightx = 0.0;
				msgOffsetField.setEditable(false);
				msgPanel.add(msgOffsetField, gbc);
				gbc.gridx = 1;
				gbc.weightx = 1.0;
				gbc.gridwidth = 2;
				msgKeyField.setEditable(false);
				msgPanel.add(msgKeyField, gbc);
				gbc.gridwidth = 1;
				gbc.gridy = 1;
				gbc.gridx = 0;
				gbc.weightx = 0.0;
				msgPanel.add(msgViewHex, gbc);
				gbc.gridx = 1;
				gbc.weightx = 1.0;
				msgPanel.add(msgPostProcessor, gbc);
				gbc.gridx = 2;
				gbc.weightx = 1.0;
				msgPanel.add(msgViewEncoding, gbc);
				gbc.gridy = 2;
				gbc.gridx = 0;
				gbc.weightx = 1.0;
				gbc.weighty = 1.0;
				gbc.gridwidth = 3;
				gbc.fill = GridBagConstraints.BOTH;
				msgContent.setEditable(false);
				JTabbedPane tabPane = new JTabbedPane();
				tabPane.addTab("Message content", new JScrollPane(msgContent));
				tabPane.addTab("Groovy processor", new JScrollPane(txaGroovyTransform));
				msgPanel.add(tabPane, gbc);

				gbc = new GridBagConstraints();
				gbc.fill = GridBagConstraints.HORIZONTAL;
				gbc.weighty = 0.0;
				gbc.gridy = 0;
				gbc.gridx = 0;
				gbc.weightx = 0.0;
				topicMessagesPanel.add(msgGetOption, gbc);

				gbc.gridy = 0;
				gbc.gridx = 1;
				gbc.weightx = 0.2;
				SwingUtil.minPrefWidth(msgGetCount, 64);
				topicMessagesPanel.add(msgGetCount, gbc);

				gbc.gridy = 0;
				gbc.gridx = 2;
				gbc.weightx = 0.0;
				topicMessagesPanel.add(btnGetMessages, gbc);

				gbc.gridy = 0;
				gbc.gridx = 3;
				gbc.weightx = 0.0;
				topicMessagesPanel.add(btnPostMessage, gbc);

				gbc.gridy = 1;
				gbc.gridx = 0;
				gbc.weightx = 0.4;
				msgReadTopic.setEditable(false);
				msgReadTopic.setBorder(BorderFactory.createTitledBorder("Topic"));
				topicMessagesPanel.add(msgReadTopic, gbc);

				gbc.gridy = 1;
				gbc.gridx = 1;
				gbc.weightx = 0.4;
				msgReadPartition.setEditable(false);
				msgReadPartition.setBorder(BorderFactory.createTitledBorder("Partition"));
				topicMessagesPanel.add(msgReadPartition, gbc);

				gbc.gridy = 1;
				gbc.gridx = 2;
				gbc.weightx = 0.2;
				msgDetectedBeginOffset.setEditable(false);
				msgDetectedBeginOffset.setBorder(BorderFactory.createTitledBorder("Earliest offset"));
				topicMessagesPanel.add(msgDetectedBeginOffset, gbc);

				gbc.gridy = 1;
				gbc.gridx = 3;
				gbc.weightx = 0.2;
				msgDetectedEndOffset.setEditable(false);
				msgDetectedEndOffset.setBorder(BorderFactory.createTitledBorder("Latest offset"));
				topicMessagesPanel.add(msgDetectedEndOffset, gbc);

				gbc.gridy = 2;
				gbc.gridx = 0;
				gbc.weightx = 1.0;
				gbc.weighty = 1.0;
				gbc.gridwidth = 6;
				gbc.fill = GridBagConstraints.BOTH;
				topicMessagesPanel.add(new JScrollPane(msgTable), gbc);

				JSplitPane msgSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, true, topicMessagesPanel, msgPanel);
				msgSplitPane.setResizeWeight(0.5);
				contentPanel.add(msgSplitPane);

				KafkaAdminGui.this.setLayout(new BorderLayout());
				KafkaAdminGui.this.add(splitPane, BorderLayout.CENTER);
				KafkaAdminGui.this.pack();
				SwingUtil.minPrefWidth(KafkaAdminGui.this, 800);
				KafkaAdminGui.this.pack();
				SwingUtil.moveToScreenCenter(this);
				msgSplitPane.setDividerLocation(0.5);

				btnGetMessages.addActionListener(actEvt -> this.ifTopicOrPartitionSelected(topicPartition -> {
					receiveInProgress = true;
					onReceiveStateChange();

					currentResults.clear();
					while (msgTableModel.getRowCount() > 0) {
						msgTableModel.removeRow(0);
					}
					msgTableModel.fireTableDataChanged();
					String countOfMsgsToRetrieve = msgGetCount.getText().replaceAll("[^0-9]+", "");
					int msgsToRetrieve;
					if (countOfMsgsToRetrieve.trim().isEmpty()) {
						msgsToRetrieve = 1;
					} else {
						msgsToRetrieve = Integer.parseInt(countOfMsgsToRetrieve.trim());
					}
					String charset = msgViewEncoding.getSelectedItem().toString();
					boolean latest = msgGetOption.getSelectedItem().toString().equalsIgnoreCase("Latest");
					SwingUtil.performSafely(() -> {
						clientConfig.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
						clientConfig.setProperty("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
						try (KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(clientConfig)) {
							Integer selectedPartition = topicPartition.getB();
							boolean partitionSelected = selectedPartition != null;
							Set<Integer> partitions;
							if (partitionSelected) {
								partitions = new HashSet<>(Arrays.asList(selectedPartition));
							} else {
								partitions = kafkaConsumer.partitionsFor(topicPartition.getA()).stream().map(PartitionInfo::partition)
										.collect(Collectors.toSet());
							}
							Executor newThreadExecutor = command -> new Thread(command).start();
							List<CompletableFuture<Void>> ops = new ArrayList<>(partitions.size());
							for (int currentPartition : partitions) {
								ops.add(CompletableFuture.runAsync(() -> {
									try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(clientConfig)) {
										TopicPartition tp = new TopicPartition(topicPartition.getA(), currentPartition);
										Long endOffset = consumer.endOffsets(Arrays.asList(tp)).get(tp);
										Long beginningOffset = consumer.beginningOffsets(Arrays.asList(tp)).get(tp);
										SwingUtilities.invokeLater(() -> {
											msgReadTopic.setText(topicPartition.getA());
											msgReadPartition.setText(partitionSelected ? String.valueOf(currentPartition) : "All");
											if (partitionSelected) {
												msgDetectedBeginOffset.setText(beginningOffset != null ? beginningOffset.toString() : "");
												msgDetectedEndOffset.setText(endOffset != null ? endOffset.toString() : "");
											} else {
												msgDetectedBeginOffset.setText("");
												msgDetectedEndOffset.setText("");
											}
										});

										if (endOffset != beginningOffset) {
											consumer.assign(Arrays.asList(tp));
											long finishAt;
											if (latest) {
												finishAt = endOffset != null ? endOffset.longValue() - 1 : 0;
												consumer.seek(tp, Math.max(endOffset - msgsToRetrieve, beginningOffset));
											} else {
												finishAt = Math.min(
														(beginningOffset != null ? beginningOffset.longValue() : 0) + msgsToRetrieve,
														endOffset) - 1;
												consumer.seek(tp, beginningOffset);
											}
											boolean done = false;
											int attemptsLeft = 6;
											while (!done && attemptsLeft-- > 0) {
												List<ConsumerRecord<String, byte[]>> page = consumer.poll(Duration.ofSeconds(5))
														.records(tp);
												currentResults.addAll(page);
												for (ConsumerRecord<String, byte[]> message : page) {
													synchronized (msgTableModel) {
														msgTableModel.addRow(new String[] { String.valueOf(message.partition()),
																String.valueOf(message.offset()), message.key(),
																new String(message.value() != null ? message.value() : new byte[0],
																		charset) });
													}
												}
												if (!page.isEmpty()) {
													long lastRecordOffset = page.get(page.size() - 1).offset();
													done = lastRecordOffset >= finishAt;
												}
											}
										}
									} catch (Exception e) {
										SwingUtil.showError("Error while reading messages", e);
									}
								}, newThreadExecutor));
							}
							CompletableFuture.allOf(ops.toArray(new CompletableFuture[ops.size()])).get();
							SwingUtilities.invokeLater(msgTableModel::fireTableDataChanged);
						} finally {
							receiveInProgress = false;
							SwingUtilities.invokeLater(() -> {
								onReceiveStateChange();
							});
						}
					});
				}));

				ActionListener alViewMessage = e -> {
					int idx = msgTable.getSelectedRow();
					if (idx >= 0 && idx < currentResults.size()) {
						ConsumerRecord<String, byte[]> record = currentResults.get(idx);
						viewMsgContent(record.value());
					}
				};
				msgViewHex.addActionListener(alViewMessage);
				msgViewEncoding.addActionListener(alViewMessage);
				msgPostProcessor.addActionListener(alViewMessage);
				msgViewHex.addActionListener(e -> {
					boolean hex = msgViewHex.isSelected();
					msgViewEncoding.setEnabled(!hex);
					msgPostProcessor.setEnabled(!hex);
				});

				msgTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
				msgTable.setRowSelectionAllowed(true);
				msgTable.setColumnSelectionAllowed(false);
				msgTable.getSelectionModel().addListSelectionListener(evt -> {
					int idx = msgTable.getSelectedRow();
					if (idx >= 0 && idx < currentResults.size()) {
						ConsumerRecord<String, byte[]> record = currentResults.get(idx);
						msgOffsetField.setText(String.valueOf(record.offset()));
						msgKeyField.setText(record.key());
						viewMsgContent(record.value());
					}
				});

				btnPostMessage.addActionListener(actEvt -> this.ifTopicOrPartitionSelected(topicPartition -> {
					JDialog postMessageDialog = new JDialog(KafkaAdminGui.this, "Post message to topic " + topicPartition.getA()
							+ (topicPartition.getB() != null ? ", partition " + topicPartition.getB() : ", any partition"), false);
					postMessageDialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
					postMessageDialog.setLayout(new BorderLayout());
					postMessageDialog.setPreferredSize(new Dimension(600, 400));
					JTextField tf = new JTextField();
					tf.setBorder(BorderFactory.createTitledBorder("Message key"));
					JTextArea txa = new JTextArea();
					txa.setBorder(BorderFactory.createTitledBorder("Message content"));
					JButton btnPost = new JButton("Post");
					JButton btnCancel = new JButton("Cancel");
					btnCancel.addActionListener(e -> {
						postMessageDialog.setVisible(false);
						postMessageDialog.dispose();
					});
					postMessageDialog.add(SwingUtil.twoComponentPanel(btnCancel, btnPost), BorderLayout.SOUTH);
					postMessageDialog.add(tf, BorderLayout.NORTH);
					postMessageDialog.add(new JScrollPane(txa), BorderLayout.CENTER);
					postMessageDialog.pack();
					SwingUtil.moveToScreenCenter(postMessageDialog);
					postMessageDialog.setVisible(true);

					btnPost.addActionListener(ae -> {
						postMessageDialog.setVisible(false);
						postMessageDialog.dispose();
						String messageKey = tf.getText().isEmpty() ? null : tf.getText();
						String messageContent = txa.getText();
						SwingUtil.performSafely(() -> {
							clientConfig.setProperty("key.serializer", StringSerializer.class.getCanonicalName());
							clientConfig.setProperty("value.serializer", ByteArraySerializer.class.getCanonicalName());
							try (KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(clientConfig)) {
								producer.send(
										new ProducerRecord<String, byte[]>(topicPartition.getA(), topicPartition.getB(), messageKey,
												messageContent.getBytes(StandardCharsets.UTF_8)),
										(metadata, exception) -> SwingUtilities.invokeLater(() -> {
											if (exception != null) {
												SwingUtil.showError("Error while submitting message", exception);
											} else {
												JOptionPane.showMessageDialog(KafkaAdminGui.this,
														"Message successfully posted to topic " + metadata.topic() + " partition "
																+ metadata.partition() + " with offset " + metadata.offset());
											}
										}));
							}
						});
					});
				}));

				btnCreateTopic.addActionListener(e -> {
					new NewTopicDialog(KafkaAdminGui.this, params -> {
						try {
							kafkaAdminClient.createTopics(Arrays.asList(new NewTopic(params.getA(), params.getB(), params.getC())));
							TopicDescription topicDescription = kafkaAdminClient.describeTopics(Arrays.asList(params.getA())).values()
									.get(params.getA()).get();
							SwingUtilities.invokeLater(() -> {
								DefaultMutableTreeNode topicNode = createTopicNode(
										KafkaTopic.builder().name(params.getA()).internal(false).build(), topicDescription);
								treeModel.insertNodeInto(topicNode, topicsRootNode, topicsRootNode.getChildCount());
								JOptionPane.showMessageDialog(KafkaAdminGui.this, "Topic " + params.getA() + " successfully created.");
							});
						} catch (Exception ex) {
							SwingUtil.showError("Error while creating topic", ex);
						}
					});
				});
				btnDeleteTopic.addActionListener(e -> {
					ifTopicOrPartitionSelected(param -> {
						String topicName = param.getA();
						if (JOptionPane.OK_OPTION == JOptionPane.showConfirmDialog(KafkaAdminGui.this,
								"Are you sure you want to delete topic " + topicName + "?", "Delete topic", JOptionPane.OK_CANCEL_OPTION)) {
							new Thread(() -> kafkaAdminClient.deleteTopics(Arrays.asList(topicName)).all().whenComplete((a, exception) -> {
								if (exception != null) {
									SwingUtil.showError("Error while deleting topic", exception);
								} else {
									JOptionPane.showMessageDialog(KafkaAdminGui.this, "Topic delete request successfully sent");
									DefaultMutableTreeNode node = param.getC();
									while (node != null && !(node.getUserObject() instanceof KafkaTopic)) {
										node = (DefaultMutableTreeNode) node.getParent();
									}
									if (node != null) {
										treeModel.removeNodeFromParent(node);
									}
								}
							})).start();
						}
					});
				});
				
				updReceiveButtonState();
				updSendButtonState();

				KafkaAdminGui.this.setVisible(true);
			});
		});
	}

	private DefaultMutableTreeNode createTopicNode(KafkaTopic topic, TopicDescription topicDescription) {
		DefaultMutableTreeNode topicNode = new DefaultMutableTreeNode(topic, true);

		DefaultMutableTreeNode partitionsNode = new DefaultMutableTreeNode("Partitions", true);
		DefaultMutableTreeNode aclsNode = new DefaultMutableTreeNode("Authorized operations", true);

		topicNode.add(partitionsNode);
		topicNode.add(aclsNode);

		if (topicDescription != null) {
			if (topicDescription.partitions() != null) {
				topicDescription.partitions().stream()
						.map(p -> KafkaTopicPartition.builder().topic(topic.getName()).number(p.partition()).build())
						.forEach(partition -> partitionsNode.add(new DefaultMutableTreeNode(partition, false)));
			}
			if (topicDescription.authorizedOperations() != null) {
				topicDescription.authorizedOperations().stream().map(AclOperation::name)
						.forEach(opName -> aclsNode.add(new DefaultMutableTreeNode(opName, false)));
			}
		}
		return topicNode;
	}

	protected void viewMsgContent(byte[] messageContent) {
		try {
			if (msgViewHex.isSelected()) {
				msgContent.setLineWrap(true);
				msgContent.setWrapStyleWord(true);
				msgContent.setFont(monospacedFont);
				msgContent.setText(HexUtil.toHex(messageContent, " "));
			} else {
				msgContent.setFont(defaultFont);
				msgContent.setLineWrap(false);
				String charset = msgViewEncoding.getSelectedItem().toString();
				String postProcessor = msgPostProcessor.getSelectedItem().toString();
				if (postProcessor.equalsIgnoreCase("None")) {
					msgContent.setText(new String(messageContent != null ? messageContent : new byte[0], charset));
				} else {
					msgContent.setText("Loading...");
					SwingUtil.performSafely(() -> {
						String errorText = null;
						byte[] messageContentProcessed = messageContent;
						try {
							messageContentProcessed = processContent(postProcessor, messageContent);
						} catch (Exception e) {
							errorText = "Error occurred: " + StackTraceUtil.toString(e);
						}
						String messageText = new String(messageContentProcessed, charset);
						String finalErrorText = errorText;
						SwingUtilities.invokeLater(() -> {
							msgContent.setText(messageText);
							msgContent.setToolTipText(finalErrorText);
							msgContent.setForeground(
									finalErrorText != null ? Color.red : (Color) UIManager.getDefaults().get("TextArea.foreground"));
						});
					});
				}
			}
		} catch (UnsupportedEncodingException e1) {
			// Should never happen because we choose an encoring from the list of supported encodings (charsets)
			e1.printStackTrace();
		}
	}

	protected byte[] processContent(String postProcessorName, byte[] content) {
		// TODO: apply strategy pattern when refactoring
		if (content != null) {
			if (postProcessorName.equals("JSON pretty-print")) {
				if (content.length > 0 && (content[0] == '{' || content[0] == '['))
					try {
						content = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(objectMapper.readTree(content));
					} catch (Exception e) {
						// Don't bother user with error - just leave content as is, unformatted
						e.printStackTrace();
					}
			} else if (postProcessorName.equals("Groovy script")) {
				Object result = Eval.me("content", content, txaGroovyTransform.getText());
				if (result instanceof byte[]) {
					content = (byte[]) result;
				} else if (result != null) {
					content = result.toString().getBytes(StandardCharsets.UTF_8);
				} else {
					content = new byte[0];
				}
			}
		} else {
			return new byte[0];
		}
		return content;
	}

	protected Tuple<String, Integer, DefaultMutableTreeNode, Void, Void> isTopicOrPartitionSelected() {
		Object selectedObject = topicsTree.getLastSelectedPathComponent();
		while (selectedObject != null) {
			if (selectedObject instanceof DefaultMutableTreeNode
					&& ((DefaultMutableTreeNode) selectedObject).getUserObject() instanceof KafkaTopicPartition) {
				KafkaTopicPartition partitionModel = (KafkaTopicPartition) ((DefaultMutableTreeNode) selectedObject).getUserObject();
				String topic = partitionModel.getTopic();
				Integer partition = partitionModel.getNumber();
				return Tuple.<String, Integer, DefaultMutableTreeNode, Void, Void> builder().a(topic).b(partition)
						.c((DefaultMutableTreeNode) selectedObject).build();
			} else if (selectedObject instanceof DefaultMutableTreeNode
					&& ((DefaultMutableTreeNode) selectedObject).getUserObject() instanceof KafkaTopic) {
				KafkaTopic topicModel = (KafkaTopic) ((DefaultMutableTreeNode) selectedObject).getUserObject();
				return Tuple.<String, Integer, DefaultMutableTreeNode, Void, Void> builder().a(topicModel.getName())
						.c((DefaultMutableTreeNode) selectedObject).build();
			} else if (selectedObject instanceof TreeNode) {
				selectedObject = ((TreeNode) selectedObject).getParent();
			}
		}
		return null;
	}

	protected void ifTopicOrPartitionSelected(Consumer<Tuple<String, Integer, DefaultMutableTreeNode, Void, Void>> action) {
		Tuple<String, Integer, DefaultMutableTreeNode, Void, Void> selection = isTopicOrPartitionSelected();
		if (selection == null) {
			JOptionPane.showMessageDialog(this, "Please select a topic or partition");
		} else {
			action.accept(selection);
		}
	}

	protected void updReceiveButtonState() {
		btnGetMessages.setEnabled(!receiveInProgress && topicOrPartitionSelected);
	}

	protected void updSendButtonState() {
		btnPostMessage.setEnabled(topicOrPartitionSelected);
	}

	protected void onReceiveStateChange() {
		updReceiveButtonState();
	}

	protected void onTopicsTreeSelectionChange() {
		topicOrPartitionSelected = isTopicOrPartitionSelected() != null;
		updReceiveButtonState();
		updSendButtonState();
	}
}
