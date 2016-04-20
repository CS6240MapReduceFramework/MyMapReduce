package hadoop;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {

	public Properties prop;
	//public FileOutputStream output;
	
	public Properties getProp() {
		return prop;
	}

	public void setProp(Properties prop) {
		this.prop = prop;
	}

	public void loadProperties(String propFileName) throws IOException
	{
		prop = new Properties();
		InputStream input = new FileInputStream(propFileName);
		prop.load(input);
		input.close();
	}
}
