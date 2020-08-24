package x.mvmn.kafkagui;

import java.io.File;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;

import x.mvmn.kafkagui.gui.ConnectionsManagerWindow;
import x.mvmn.kafkagui.gui.util.SwingUtil;
import x.mvmn.kafkagui.lang.CallUtil;

public class MVMnKafkaGUIApplication {

	public static void main(String args[]) {
		File userHome = new File(System.getProperty("user.home"));
		File appHomeFolder = new File(userHome, ".mvmnkafkagui");
		if (!appHomeFolder.exists()) {
			appHomeFolder.mkdir();
		}
		SortedSet<String> existingConnectionConfigs = Arrays.asList(appHomeFolder.listFiles()).stream().map(File::getName)
				.filter(fn -> fn.toLowerCase().endsWith(".properties")).map(fn -> fn.substring(0, fn.length() - ".properties".length()))
				.collect(Collectors.toCollection(TreeSet::new));

		JFrame connectionsManagerWindow = new ConnectionsManagerWindow(appHomeFolder, existingConnectionConfigs, cfg -> {
			CallUtil.doSafely(() -> {
				AdminClient ac = KafkaAdminClient.create(cfg);
				// Perform list topics as a test
				ac.listTopics().names().get();
				SwingUtilities.invokeLater(() -> {
					JOptionPane.showMessageDialog(null, "Connection successfull");
				});
			});
		}, cfg -> {});
		connectionsManagerWindow.pack();
		SwingUtil.moveToScreenCenter(connectionsManagerWindow);
		connectionsManagerWindow.setVisible(true);

		// KafkaConfigPanel kcp = new KafkaConfigPanel();
		// JButton connect = new JButton("Connect");
		// connect.addActionListener(e -> {
		// try {
		// ByteArrayOutputStream baos = new ByteArrayOutputStream();
		// Properties props = kcp.getCurrentState().modelToProperties();
		// props.store(baos, "");
		// System.out.println(new String(baos.toByteArray(), StandardCharsets.UTF_8));
		// try (AdminClient ac = KafkaAdminClient.create(props)) {
		// ac.listTopics().names().get().stream().forEach(System.out::println);
		// }
		// } catch (Exception ex) {
		// ex.printStackTrace();
		// }
		// });
		//
		// JFrame frame = new JFrame("MVMn Kafka Client GUI");
		// frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		// frame.setLayout(new BorderLayout());
		// frame.add(new JScrollPane(kcp), BorderLayout.CENTER);
		// frame.add(connect, BorderLayout.SOUTH);
		// frame.pack();
		// frame.setVisible(true);

	}
}
