package com.microsoft.azure.datalake.store.protocol;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Hashtable;


/**
 * Internal class for SDK's internal use. DO NOT USE.
 */
public class QueryParams {

    private Hashtable<String, String> params = new Hashtable<String, String>();
    Operation op = null;
    String apiVersion = null;
    String separator = "";

    public void add(String name, String value) {
        params.put(name, value);
    }

    public void setOp(Operation op) {
        this.op = op;
    }

    public void setApiVersion(String apiVersion) { this.apiVersion = apiVersion; }

    public String serialize()  {
        StringBuilder sb = new StringBuilder();

        if (op != null) {
            sb.append(separator);
            sb.append("op="); sb.append(op.name);
            separator = "&";
        }

        for (String name : params.keySet()) {
            try {
                sb.append(separator);
                sb.append(URLEncoder.encode(name, "UTF-8"));
                sb.append('=');
                sb.append(URLEncoder.encode(params.get(name), "UTF-8"));
                separator = "&";
            } catch (UnsupportedEncodingException ex) { }
        }

        if (apiVersion != null) {
            sb.append(separator);
            sb.append("api-version="); sb.append(apiVersion);
            separator = "&";
        }

        return sb.toString();
    }
}
