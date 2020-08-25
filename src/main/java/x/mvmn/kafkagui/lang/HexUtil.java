package x.mvmn.kafkagui.lang;

public class HexUtil {

	private static final String[] HEX_VALS_256;
	private static final char[] HEX_VALS_16 = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
	static {
		HEX_VALS_256 = new String[256];
		for (int i = 0; i < 256; i++) {
			int l = i & 0x0F;
			int h = (i >> 4) & 0x0F;
			HEX_VALS_256[i] = String.valueOf(HEX_VALS_16[h]) + String.valueOf(HEX_VALS_16[l]);
		}
	}

	public static String toHex(byte[] data, String separator) {
		if (data == null) {
			return null;
		}
		StringBuilder result = new StringBuilder();
		for (byte b : data) {
			int v = b & 0xFF;
			result.append(HEX_VALS_256[v]);
			if (separator != null) {
				result.append(separator);
			}
		}

		return result.toString();
	}
}
