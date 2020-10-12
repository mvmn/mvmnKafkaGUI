package x.mvmn.kafkagui.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

public class FileBackedProperties {

	protected final File backingFile;
	protected final Properties properties;

	public FileBackedProperties(File backingFile) {
		this.backingFile = backingFile;
		this.properties = new Properties();
		if (backingFile.exists()) {
			try (FileInputStream fis = new FileInputStream(backingFile)) {
				properties.load(fis);
			} catch (Exception e) {
				throw new RuntimeException("Failed to load properties from file " + backingFile.getAbsolutePath(), e);
			}
		}
	}

	public String getProperty(String property) {
		return properties.getProperty(property);
	}

	public String getProperty(String property, String defaultValue) {
		return properties.getProperty(property, defaultValue);
	}

	public FileBackedProperties setProperty(String property, String value) {
		properties.setProperty(property, value);
		try (FileOutputStream fos = new FileOutputStream(backingFile, false)) {
			properties.store(fos, "");
		} catch (Exception e) {
			throw new RuntimeException("Failed to save properties to file " + backingFile.getAbsolutePath(), e);
		}

		return this;
	}
}
