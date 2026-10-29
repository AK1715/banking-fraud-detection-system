package com.banking.notificationservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGenerated(
            @Payload Map<String, Object> payload) {

        log.info("OTP event received: {}", payload);

        try {
            String accountNumber =
                    String.valueOf(payload.get("accountNumber"));
            String otp = String.valueOf(payload.get("otp"));
            String amount = String.valueOf(payload.get("amount"));
            String reason = String.valueOf(payload.get("reason"));

            String message = String.format(
                    "Suspicious activity detected. Reason: %s. " +
                            "A transaction of %s is pending verification. " +
                            "Your OTP is %s. Valid for 5 minutes.",
                    reason, amount, otp
            );

            sendAlert(
                    accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    message
            );

        } catch (Exception e) {
            log.error("Error sending OTP notification", e);
            throw e;
        }
    }


    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload) {

        try {
            String senderAccount =
                    String.valueOf(payload.get("senderAccountNumber"));
            String receiverAccount =
                    String.valueOf(payload.get("receiverAccountNumber"));
            String amount = String.valueOf(payload.get("amount"));

            log.info("Transaction completed event received: {}", payload);

            // Debit alert to sender
            sendAlert(
                    senderAccount,
                    "DEBIT ALERT",
                    String.format(
                            "%s debited from your account %s.",
                            amount, senderAccount
                    )
            );

            // Credit alert to receiver
            sendAlert(
                    receiverAccount,
                    "CREDIT ALERT",
                    String.format(
                            "%s credited to your account %s from account %s.",
                            amount, receiverAccount, senderAccount
                    )
            );

        } catch (Exception e) {
            log.error("Error sending transaction notification", e);
            throw e;
        }
    }


    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload){
        try {

            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,
                    "SUSPICIOUS ACTIVITY DETECTED",
                    String.format(
                            "Your account %s has been blocked" +
                            "Reason %s " +
                            "Please contact your bank immediately",
                            accountNumber, reason
                    ));

        } catch (Exception e) {
            log.error("Error sending Fraud alert: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(
            @Payload Map<String, Object> payload){
        try{

            String senderAccount = (String) payload.get("senderAccountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(senderAccount,
                   "REFUND PROCESSED",
                   String.format(
                           "Your transaction of %s was cancelled. " +
                           "Reason: %s " +
                           "%s has been refunded to account %s",
                           amount, reason, amount, senderAccount
                   ));


        }
        catch (Exception e){
            log.error("Error sending refund notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(
            @Payload Map<String, Object> payload){
        try {

            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber,
                    "PAYMENT SUCCESSFULL",
                    String.format(
                            "Payment of %s completed "+
                            "Razorpay ID: %s ",
                            amount, payload.get("razorpayPaymentId")
                    ));

        } catch (Exception e) {
            log.error("Error sending payment notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(
            @Payload Map<String, Object> payload){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber,
                    "PAYMENT FAILED",
                    String.format(
                            "Your payment of %d could not be processed. " +
                                    "Please try again or contact support.",
                            amount
                    ));
        } catch (Exception e) {
            log.error("Error sending payment failure notification: {}", e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String subject, String message){
        log.info("========== BANK NOTIFICATION ==========");
        log.info("Account  : {}", accountNumber);
        log.info("Subject  : {}", subject);
        log.info("Message  : {}", message);
        log.info("========================================");
    }

}
