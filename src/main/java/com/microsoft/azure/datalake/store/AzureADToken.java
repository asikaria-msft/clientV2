package com.microsoft.azure.datalake.store;

import java.time.LocalDateTime;


public class AzureADToken {
    public String accessToken;
    public String refreshToken;
    @SuppressWarnings("Since15")
    public LocalDateTime expiry;
}