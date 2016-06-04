package com.microsoft.azure.datalake.store.protocol;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Hashtable;


public class QueryParams {

    private Hashtable<String, String> params = new Hashtable<String, String>();
    Operation op = null;

    public void add(String name, String value) {
        params.put(name, value);
    }

    public void setOp(Operation op) {
        this.op = op;
    }

    public String serialize()  {
        StringBuilder sb = new StringBuilder();

        if (op != null) {
            sb.append("op="); sb.append(op.name);
        }

        for (String name : params.keySet()) {
            try {
                sb.append('&');
                sb.append(URLEncoder.encode(name, "UTF-8"));
                sb.append('=');
                sb.append(URLEncoder.encode(params.get(name), "UTF-8"));
            } catch (UnsupportedEncodingException ex) { }
        }
        return sb.toString();
    }

}
