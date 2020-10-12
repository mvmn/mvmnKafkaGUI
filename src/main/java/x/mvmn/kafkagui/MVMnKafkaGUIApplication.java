package x.mvmn.kafkagui;

import java.io.File;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;

import com.formdev.flatlaf.FlatDarculaLaf;
import com.formdev.flatlaf.FlatDarkLaf;
import com.formdev.flatlaf.FlatIntelliJLaf;
import com.formdev.flatlaf.FlatLightLaf;

import x.mvmn.kafkagui.gui.ConnectionsManagerWindow;
import x.mvmn.kafkagui.gui.KafkaAdminGui;
import x.mvmn.kafkagui.gui.util.SwingUtil;
import x.mvmn.kafkagui.lang.CallUtil;
import x.mvmn.kafkagui.util.FileBackedProperties;

public class MVMnKafkaGUIApplication {

	public static void main(String args[]) {
		File userHome = new File(System.getProperty("user.home"));
		File appHomeFolder = new File(userHome, ".mvmnkafkagui");
		if (!appHomeFolder.exists()) {
			appHomeFolder.mkdir();
		}
		SortedSet<String> existingConnectionConfigs = Arrays.asList(appHomeFolder.listFiles()).stream().filter(File::isFile)
				.map(File::getName).filter(fn -> fn.toLowerCase().endsWith(".properties"))
				.map(fn -> fn.substring(0, fn.length() - ".properties".length())).collect(Collectors.toCollection(TreeSet::new));
		File configFile = new File(appHomeFolder, "config.cfg");
		FileBackedProperties appConfig = new FileBackedProperties(configFile);

		String lookAndFeelName = appConfig.getProperty("gui.lookandfeel");
		SwingUtilities.invokeLater(() -> {
			Stream.of(FlatLightLaf.class, FlatIntelliJLaf.class, FlatDarkLaf.class, FlatDarculaLaf.class)
					.forEach(lafClass -> UIManager.installLookAndFeel(lafClass.getSimpleName(), lafClass.getCanonicalName()));

			if (lookAndFeelName != null) {
				SwingUtil.setLookAndFeel(lookAndFeelName);
			}

			JFrame connectionsManagerWindow = new ConnectionsManagerWindow(appConfig, appHomeFolder, existingConnectionConfigs,
					CallUtil.unsafe(cfg -> {
						AdminClient ac = KafkaAdminClient.create(cfg);
						// Perform list topics as a test
						ac.listTopics().names().get();
						SwingUtilities.invokeLater(() -> {
							JOptionPane.showMessageDialog(null, "Connection successfull");
						});
					}), cfg -> new KafkaAdminGui(cfg.getA(), cfg.getB(), appHomeFolder));
			SwingUtil.prefSizeRatioOfScreenSize(connectionsManagerWindow, 0.7f);
			connectionsManagerWindow.pack();
			SwingUtil.moveToScreenCenter(connectionsManagerWindow);
			connectionsManagerWindow.setVisible(true);
		});
	}
}
