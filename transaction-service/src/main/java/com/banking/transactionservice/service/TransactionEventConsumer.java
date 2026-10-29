package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {
    private final TransactionRepository transactionRepository;
    private final RedisTemplate<String, String> redisTemplate;
    private static final long OTP_EXPIRY_MINUTES = 5;
    private final TransactionService transactionService;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";
    /*
    * Consume verification is required
    * Generate OTP and ask user to verify
    * @param payload
    */
    @KafkaListener(topics = "verification.required")
    public void consumeVerificationRequired(
            @Payload Map<String, Object> payload){
        try{

            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            log.info("verification require - transaction: {} reason: {}",
                    transactionId, reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                    .orElseThrow(() -> new RuntimeException("Transaction Not Found " + transactionId));

            if(transaction.getStatus() != TransactionStatus.PROCESSING){
                log.warn("Transaction: {} not PROCESSING",  transactionId);
                return;
            }

            // Generate 6 digit otp
            String otp = String.format("%06d", (int) (Math.random() * 900000) + 100000);

//            log.info("OPT is Here : {}", otp);

            // Store OTP in Redis - expires in 5 minutes
            String otpKey = "verification:otp" + transactionId;
            redisTemplate.opsForValue().set(otpKey, otp, OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);

            // updated status
            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP Generated for transaction id: {} expires in {} min",
                    transactionId, OTP_EXPIRY_MINUTES);

            // Notify user
            Map<String, Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("reason", reason);
            otpEvent.put("otp", otp);
            otpEvent.put("amount", payload.get("amount"));

            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, transactionId, otpEvent);

        } catch (Exception e) {
            log.error("Error handling verification required: {}" , e.getMessage());
        }
    }

    @KafkaListener(
            topics = "fraud.check.clean",
            groupId = "transaction-service-group"
    )
    public void consumeFraudCheckCleanResult(
            @Payload Map<String, Object> payload) {

        log.info("FRAUD CLEAN EVENT RECEIVED: {}", payload);

        try {
            Object id = payload.get("transactionId");
            if (id == null) {
                throw new IllegalArgumentException(
                        "Missing transactionId in fraud clean event");
            }

            String transactionId = String.valueOf(id);

            transactionService.processCleanResult(transactionId);

            log.info("Fraud clean result processed: {}", transactionId);

        } catch (Exception e) {
            log.error("Error processing fraud clean result. Payload: {}",
                    payload, e);
            throw e;
        }
    }
}
