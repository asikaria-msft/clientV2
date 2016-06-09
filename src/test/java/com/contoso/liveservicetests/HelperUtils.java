package com.contoso.liveservicetests;


import java.io.FileInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Properties;

public class HelperUtils {

    private static Properties prop = null;
    public static Properties getProperties() throws IOException {
        if (prop==null) {
            Properties defaultProps = new Properties();
            defaultProps.load(new FileInputStream("config.properties"));
            prop = new Properties(defaultProps);
            prop.load(new FileInputStream("creds.properties"));
        }
        return prop;
    }

    private static byte[] buf4mb = null;
    public static byte[] getRandom4mbBuffer() {
        if (buf4mb == null) {
            SecureRandom prng = new SecureRandom();
            prng.nextBytes(buf4mb);
        }
        return buf4mb;
    }
}
