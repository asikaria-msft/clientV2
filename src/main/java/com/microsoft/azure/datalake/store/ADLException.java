/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;


import java.io.IOException;

public class ADLException extends IOException {

    public int httpResponseCode;
    public String httpResponseMessage;
    public String requestId = null;

    public int numRetries;
    public long lastCallLatency = 0;
    public long responseContentLength = 0;

    public String remoteExceptionName = null;
    public String remoteExceptionMessage = null;
    public String remoteExceptionJavaClassName = null;

    public ADLException(String message, Throwable initCause) {
        super(message, initCause);


    }



}
