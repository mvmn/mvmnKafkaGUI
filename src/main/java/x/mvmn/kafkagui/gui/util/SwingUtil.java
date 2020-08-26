package x.mvmn.kafkagui.gui.util;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.Toolkit;

import javax.swing.JPanel;
import javax.swing.SwingUtilities;

import x.mvmn.kafkagui.lang.StackTraceUtil;
import x.mvmn.kafkagui.lang.UnsafeOperation;

public class SwingUtil {

	protected static ErrorMessageDialog errorMessageDialog = new ErrorMessageDialog(null);

	public static void performSafely(final UnsafeOperation operation) {
		new Thread(new Runnable() {
			public void run() {
				try {
					operation.run();
				} catch (final Exception e) {
					e.printStackTrace();
					SwingUtilities.invokeLater(new Runnable() {
						public void run() {
							errorMessageDialog.show(null, "Error occurred: " + e.getClass().getName() + " " + e.getMessage(),
									StackTraceUtil.toString(e));
						}
					});
				}
			}
		}).start();
	}

	public static void showError(final String message, final Throwable e) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				errorMessageDialog.show(null, message + ": " + e.getClass().getName() + " " + e.getMessage(), StackTraceUtil.toString(e));
			}
		});
	}

	public static void moveToScreenCenter(final Component component) {
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
		Dimension componentSize = component.getSize();
		int newComponentX = screenSize.width - componentSize.width;
		if (newComponentX >= 0) {
			newComponentX = newComponentX / 2;
		} else {
			newComponentX = 0;
		}
		int newComponentY = screenSize.height - componentSize.height;
		if (newComponentY >= 0) {
			newComponentY = newComponentY / 2;
		} else {
			newComponentY = 0;
		}
		component.setLocation(newComponentX, newComponentY);
	}

	public static JPanel twoComponentPanel(Component a, Component b) {
		JPanel result = new JPanel(new GridLayout(1, 2));

		result.add(a);
		result.add(b);

		return result;
	}

	public static <T extends Component> T prefSizeRatioOfScreenSize(T component, float ratio) {
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
		Dimension prefSize = new Dimension((int) (screenSize.width * ratio), (int) (screenSize.height * ratio));
		component.setPreferredSize(prefSize);
		return component;
	}

	public static <T extends Component> T minPrefWidth(T component, int minimumPreferredWidth) {
		component.setPreferredSize(
				new Dimension(Math.max(minimumPreferredWidth, component.getPreferredSize().width), component.getPreferredSize().height));
		return component;
	}
}
