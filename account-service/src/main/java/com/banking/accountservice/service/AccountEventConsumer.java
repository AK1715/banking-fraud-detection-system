package com.banking.accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;

    /*
    * Consume transaction.completed event from Kafka
    * Credits receiver account
    * @Param payload
    */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload){
        try{

            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting account: {} amount: {}", receiverAccount, amount);
            accountService.creditBalance(receiverAccount, amount);


        } catch (Exception e) {
            log.error("Error crediting account: {}", e.getMessage());
        }
    }

    /*
    * Consume fraud.detected event from kafka
    * Blocks the flagged account
    * @Param payload
    */
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload){
        try{

            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected - Blocking account: {}", accountNumber);

            accountService.blockAccount(accountNumber);
            log.info("Account Blocked: {}", accountNumber);

        } catch (Exception e) {
            log.error("Error Blocking Account: {}",e.getMessage());
        }
    }

}
