/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import java.util.Calendar;


/**
 * Object represnting the AAD access token to use when making HTTP requests to Azure Data Lake Storage.
 */
public class AzureADToken {
    public String accessToken;
    public String refreshToken;
    public Calendar expiry;
}