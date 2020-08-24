package x.mvmn.kafkagui.gui.util;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

public class ErrorMessageDialog extends JDialog {
	private static final long serialVersionUID = -5077221715862551311L;

	protected final JLabel label = new JLabel();
	protected final JTextArea textArea = new JTextArea();
	protected final JButton btn = new JButton("Ok");

	public ErrorMessageDialog(JFrame parent) {
		super(parent, true);
		this.setDefaultCloseOperation(JDialog.HIDE_ON_CLOSE);

		this.getContentPane().setLayout(new BorderLayout());
		this.getContentPane().add(label, BorderLayout.NORTH);
		this.getContentPane().add(new JScrollPane(textArea), BorderLayout.CENTER);
		this.getContentPane().add(btn, BorderLayout.SOUTH);

		btn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				ErrorMessageDialog.this.setVisible(false);
			}
		});
	}

	public void show(String title, String message, String details) {
		setTitle(title != null ? title : "Error occurred");
		label.setText(message != null ? message : "");
		textArea.setText(details != null ? details : "");
		this.pack();
		SwingUtil.moveToScreenCenter(this);
		this.setVisible(true);
	}
}
