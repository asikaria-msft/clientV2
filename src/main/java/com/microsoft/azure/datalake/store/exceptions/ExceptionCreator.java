/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.exceptions;


import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;

import java.io.IOException;
import java.lang.reflect.Constructor;

public class ExceptionCreator {

    public IOException getExceptionFromResp(ADLStoreClient client, OperationResponse resp, String defaultMessage) {
        if (client.remoteExceptionsEnabled() &&
                resp.remoteExceptionJavaClassName !=null &&
                !resp.remoteExceptionJavaClassName.equals("")) {
            return getRemoteException(resp.remoteExceptionJavaClassName, resp.remoteExceptionMessage);
        } else {
            String msg = (resp.message == null) ? defaultMessage : resp.message;
            ADLException ex = new ADLException(msg);
            ex.httpResponseCode = resp.httpResponseCode;
            ex.httpResponseMessage = resp.httpResponseMessage;
            ex.requestId = resp.requestId;
            ex.numRetries = resp.numRetries;
            ex.lastCallLatency = resp.lastCallLatency;
            ex.responseContentLength = resp.responseContentLength;
            ex.remoteExceptionName = resp.remoteExceptionName;
            ex.remoteExceptionMessage = resp.remoteExceptionMessage;
            ex.remoteExceptionJavaClassName = resp.remoteExceptionJavaClassName;
            ex.initCause(resp.ex);
            return ex;
        }
    }

    protected IOException getRemoteException(String className, String message) {
        try {
            Class clazz = Class.forName(className);
            if (!IOException.class.isAssignableFrom(clazz)) { return new IOException(message); }
            Constructor c = clazz.getConstructor(String.class);
            return (IOException) c.newInstance(message);
        } catch (Exception ex) {
            return new IOException(message);
        }
    }

}
