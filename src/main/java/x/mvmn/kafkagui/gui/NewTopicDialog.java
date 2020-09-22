package x.mvmn.kafkagui.gui;

import java.awt.Frame;
import java.awt.GridLayout;
import java.util.function.Consumer;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JTextField;

import x.mvmn.kafkagui.gui.util.SwingUtil;
import x.mvmn.kafkagui.lang.Tuple;

public class NewTopicDialog extends JDialog {
	private static final long serialVersionUID = 7289607389286501328L;

	public NewTopicDialog(Frame owner, Consumer<Tuple<String, Integer, Short, Void, Void>> callback) {
		super(owner, "Create new topic", false);
		this.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		this.setLayout(new GridLayout(4, 1));
		JTextField tfTopicName = new JTextField();
		tfTopicName.setBorder(BorderFactory.createTitledBorder("Topic name"));
		JTextField tfPartitions = SwingUtil.numericOnlyTextField(1L, 0L, 0L + Integer.MAX_VALUE, false);
		tfPartitions.setBorder(BorderFactory.createTitledBorder("Number of partitions"));
		JTextField tfReplicas = SwingUtil.numericOnlyTextField(1L, 0L, 0L + Short.MAX_VALUE, false);
		tfReplicas.setBorder(BorderFactory.createTitledBorder("Number of replicas"));
		JButton btnCreate = new JButton("Create");
		JButton btnCancel = new JButton("Cancel");
		btnCancel.addActionListener(e -> {
			this.setVisible(false);
			this.dispose();
		});
		btnCreate.addActionListener(ae -> {
			Tuple<String, Integer, Short, Void, Void> input = new Tuple<>(tfTopicName.getText(), Integer.parseInt(tfPartitions.getText()),
					Short.parseShort(tfReplicas.getText()), null, null);
			new Thread(() -> callback.accept(input)).start();
			this.setVisible(false);
			this.dispose();
		});
		this.add(tfTopicName);
		this.add(tfPartitions);
		this.add(tfReplicas);
		this.add(SwingUtil.twoComponentPanel(btnCancel, btnCreate));
		this.pack();
		SwingUtil.moveToScreenCenter(this);
		this.setVisible(true);
	}
}
