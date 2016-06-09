package com.contoso.liveservicetests;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.*;

public class TestCore {

    Properties prop = null;
    final UUID instanceGuid = UUID.randomUUID();

    @Before
    public void setup() throws IOException {
        prop = HelperUtils.getProperties();
    }




}
