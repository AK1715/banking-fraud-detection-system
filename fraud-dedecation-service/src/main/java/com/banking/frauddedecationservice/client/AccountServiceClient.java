package com.banking.frauddedecationservice.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {

    @GetMapping("/api/v1/accounts/{accountNumber}/balance")
    BigDecimal getBalance(
            @PathVariable String accountNumber);

    @PutMapping("/api/v1/accounts/{accountNumber}/credit")
    String creditBalance(
            @PathVariable("accountNumber") String accountNumber,
            @RequestParam("amount") BigDecimal amount
    );

}
