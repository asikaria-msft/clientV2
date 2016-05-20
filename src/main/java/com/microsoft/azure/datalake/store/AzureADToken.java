package com.microsoft.azure.datalake.store;

import java.time.LocalDateTime;


public class AzureADToken {
    public String AccessToken;
    public String RefreshToken;
    @SuppressWarnings("Since15")
    public LocalDateTime expiry;
}