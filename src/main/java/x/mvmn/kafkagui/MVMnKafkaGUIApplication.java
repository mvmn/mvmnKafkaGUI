package x.mvmn.kafkagui;

import java.awt.BorderLayout;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JScrollPane;

import x.mvmn.kafkagui.gui.KafkaConfigPanel;

public class MVMnKafkaGUIApplication {

	public static void main(String args[]) {
		KafkaConfigPanel kcp = new KafkaConfigPanel();
		JButton connect = new JButton("Connect");
		connect.addActionListener(e -> {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				kcp.getCurrentState().modelToProperties().store(baos, "");
				System.out.println(new String(baos.toByteArray(), StandardCharsets.UTF_8));
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		});

		JFrame frame = new JFrame();
		frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		frame.setLayout(new BorderLayout());
		frame.add(new JScrollPane(kcp), BorderLayout.CENTER);
		frame.add(connect, BorderLayout.SOUTH);
		frame.pack();
		frame.setVisible(true);
	}
}
