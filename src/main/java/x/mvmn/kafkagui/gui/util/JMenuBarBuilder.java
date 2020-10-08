package x.mvmn.kafkagui.gui.util;

import java.awt.event.ActionListener;
import java.util.function.Consumer;

import javax.swing.Action;
import javax.swing.Icon;
import javax.swing.JCheckBoxMenuItem;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JSeparator;

import lombok.Setter;
import lombok.experimental.Accessors;

public class JMenuBarBuilder {

	private final JMenuBar menuBar = new JMenuBar();

	public JMenuBuilder menu(String text) {
		return new JMenuBuilder().text(text);
	}

	public JMenuBar build() {
		return menuBar;
	}

	@Setter
	@Accessors(fluent = true)
	public class JMenuBuilder {
		private final JMenu menu = new JMenu();

		private String text;
		private Action action;
		private ActionListener actr;

		public JMenuBarBuilder build() {
			if (text != null) {
				menu.setText(text);
			}
			if (action != null) {
				menu.setAction(action);
			}
			if (actr != null) {
				menu.addActionListener(actr);
			}

			menuBar.add(menu);
			return JMenuBarBuilder.this;
		}

		public JMenuItemBuilder item(String text) {
			return new JMenuItemBuilder().text(text);
		}

		public JMenuBuilder separator() {
			menu.add(new JSeparator());
			return this;
		}

		public JMenuBuilder vseparator() {
			menu.add(new JSeparator(JSeparator.VERTICAL));
			return this;
		}

		@Setter
		@Accessors(fluent = true)
		public class JMenuItemBuilder {
			private String text;
			private Action action;
			private ActionListener actr;
			private Icon icon;
			private Integer mnemonic;
			private boolean asCheckbox;
			private boolean checked = false;
			private Consumer<JMenuItem> process;

			public JMenuItemBuilder checkbox() {
				asCheckbox = true;
				return this;
			}

			public JMenuBuilder build() {
				JMenuItem mi;
				if (asCheckbox) {
					JCheckBoxMenuItem cbmi = new JCheckBoxMenuItem();
					cbmi.setState(checked);
					mi = cbmi;
				} else {
					mi = new JMenuItem();
				}
				if (text != null) {
					mi.setText(text);
				}
				if (action != null) {
					mi.setAction(action);
				}
				if (actr != null) {
					mi.addActionListener(actr);
				}
				if (icon != null) {
					mi.setIcon(icon);
				}
				if (mnemonic != null) {
					mi.setMnemonic(mnemonic);
				}
				if (process != null) {
					process.accept(mi);
				}

				JMenuBuilder.this.menu.add(mi);
				return JMenuBuilder.this;
			}
		}
	}
}
