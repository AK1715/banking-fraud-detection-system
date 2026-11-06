package com.banking.paymentservice.service;

import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.entity.Payment;
import com.banking.paymentservice.entity.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import com.razorpay.Utils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;

    @Value("${razorpay.webhook-secret}")
    private String webhookSecret;

    private static final String CURRENCY = "USD";

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";


    /*
    * Create Razorpay payment order.
    *
    * FLOW:
    *   1. Create order in razorpay
    *   2. Save Payment record in DB
    *   3. Return order details to frontend
    *   4. Frontend show Razorpay Checkout
    *   5. User pays.
    *   6. Razorpay calls webhook
    * @param request
    * @Return
    */
    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {

        log.info("Creating payment order for account: {} amount: {}",
                request.getAccountNumber(), request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId, keySecret);

        // Converted Amount
        int convertedAmount = request.getAmount()
                .multiply(BigDecimal.valueOf(100))
                .intValueExact();

        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount", convertedAmount);
        orderRequest.put("currency", "USD");
        orderRequest.put("receipt", "rcpt_" + System.currentTimeMillis() + UUID.randomUUID().toString()
                .replace("-", "").substring(0, 10));

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);
        log.info("Razorpay order created: {}", razorpayOrder.get("id").toString());

        // save payment record
        Payment payment = new Payment();
        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("USD");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedpayment = paymentRepository.save(payment);

        return new PaymentOrderResponse(
                savedpayment.getId(),
                razorpayOrder.get("id").toString(),
                request.getAmount(),
                "USD",
                "CREATED",
                keyId
        );

    }

//    public void handleWebhook(Map<String, Object> payload){
//        log.info("Received Razorpay webhook: {}", payload.get("event"));
//
//        String event = (String) payload.get("event");
//
//        if("payment.captured".equals(event)){
//            handlePaymentSuccess(payload);
//        }
//        else if("payment.failed".equals(event)){
//            handlePaymentFailure(payload);
//        }
//
//    }

    public void handleWebhook(String rawBody, String signature) {
        try {
            Utils.verifyWebhookSignature(
                    rawBody,
                    signature,
                    webhookSecret
            );
        } catch (Exception e) {
            throw new SecurityException(
                    "Invalid Razorpay webhook signature", e
            );
        }

        // Parse the payload only after signature verification.
        Map<String, Object> payload = new JSONObject(rawBody).toMap();

        String eventName = String.valueOf(payload.get("event"));

        log.info("Verified Razorpay webhook event: {}", eventName);

        switch (eventName) {
            case "payment.captured" ->
                    handlePaymentSuccess(payload);

            case "payment.failed" ->
                    handlePaymentFailure(payload);

            default ->
                    log.info("Ignoring unsupported Razorpay event: {}", eventName);
        }
    }

//    private void handlePaymentSuccess(Map<String, Object> payload){
//        try{
//
//            Map<String, Object> paymentData = extractPaymentData(payload);
//            String orderId = (String) paymentData.get("order_id");
//            String paymentId = (String) paymentData.get("id");
//
//            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
//                    .orElseThrow(() -> new RuntimeException("Payment Not Found for Order"));
//
//            payment.setRazorpayPaymentId(paymentId);
//            payment.setStatus(PaymentStatus.COMPLETED);
//            paymentRepository.save(payment);
//
//            // Publish payment complete event
//            Map<String, Object> event = new Hashtable<>();
//            event.put("paymentId" , payment.getId());
//            event.put("accountNumber", payment.getAccountNumber());
//            event.put("amount", payment.getAmount());
//            event.put("razorpayPaymentId", paymentId);
//
//            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), event);
//            log.info("Payment completed: {}", payment.getId());
//
//        } catch (Exception e) {
//            log.error("Error handling payment success: {}", e.getMessage());
//        }
//    }

    private void handlePaymentSuccess(Map<String, Object> payload) {
        Map<String, Object> paymentData = extractPaymentData(payload);

        String orderId = String.valueOf(paymentData.get("order_id"));
        String paymentId = String.valueOf(paymentData.get("id"));

        Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                .orElseThrow(() ->
                        new IllegalStateException(
                                "Payment not found for order: " + orderId
                        ));

        // Basic duplicate-delivery guard.
        if (payment.getStatus() == PaymentStatus.COMPLETED) {
            log.info("Payment already completed; ignoring duplicate: {}",
                    payment.getId());
            return;
        }

        // Confirm the captured amount and currency match our saved order.
        Object amountObject = paymentData.get("amount");
        if (!(amountObject instanceof Number amountNumber)) {
            throw new IllegalStateException("Missing payment amount");
        }

        int expectedAmount = payment.getAmount()
                .multiply(BigDecimal.valueOf(100))
                .intValueExact();

        if (amountNumber.longValue() != expectedAmount) {
            throw new IllegalStateException("Payment amount mismatch");
        }

        String paidCurrency = String.valueOf(paymentData.get("currency"));

        if (!CURRENCY.equals(paidCurrency)
                || !CURRENCY.equals(payment.getCurrency())) {
            throw new IllegalStateException("Payment currency mismatch");
        }

        payment.setRazorpayPaymentId(paymentId);
        payment.setStatus(PaymentStatus.COMPLETED);
        paymentRepository.save(payment);

        Map<String, Object> event = new java.util.HashMap<>();
        event.put("paymentId", payment.getId());
        event.put("accountNumber", payment.getAccountNumber());
        event.put("amount", payment.getAmount());
        event.put("currency", payment.getCurrency());
        event.put("razorpayOrderId", orderId);
        event.put("razorpayPaymentId", paymentId);

        kafkaTemplate.send(
                PAYMENT_COMPLETED_TOPIC,
                String.valueOf(payment.getId()),
                event
        ).whenComplete((result, error) -> {
            if (error != null) {
                log.error("Failed to publish payment.completed for {}",
                        payment.getId(), error);
            } else {
                log.info("Published payment.completed for {}",
                        payment.getId());
            }
        });
    }

//    private void handlePaymentFailure(Map<String, Object> payload){
//        try{
//
//            Map<String, Object> paymentData = extractPaymentData(payload);
//            String orderId = (String) paymentData.get("order_id");
//
//            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
//                    .orElseThrow(() -> new RuntimeException("Payment Not Found for Order"));
//
//            payment.setStatus(PaymentStatus.FAILED);
//            payment.setFailureReason("Payment failed via Razorpay");
//            paymentRepository.save(payment);
//
//            // Publish payment complete event
//            Map<String, Object> event = new Hashtable<>();
//            event.put("paymentId" , payment.getId());
//            event.put("accountNumber", payment.getAccountNumber());
//            event.put("amount", payment.getAmount());
//            event.put("reason", "Payment failed via Razorpay");
//
//            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), event);
//            log.warn("Payment failed: {}", payment.getId());
//
//        } catch (Exception e) {
//            log.error("Error handling payment failure: {}", e.getMessage());
//        }
//    }

    private void handlePaymentFailure(Map<String, Object> payload) {
        Map<String, Object> paymentData = extractPaymentData(payload);

        String orderId = String.valueOf(paymentData.get("order_id"));

        Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                .orElseThrow(() ->
                        new IllegalStateException(
                                "Payment not found for order: " + orderId
                        ));

        if (payment.getStatus() == PaymentStatus.COMPLETED) {
            log.warn("Ignoring failure event for completed payment: {}",
                    payment.getId());
            return;
        }

        payment.setStatus(PaymentStatus.FAILED);
        payment.setFailureReason("Payment failed via Razorpay");
        paymentRepository.save(payment);

        Map<String, Object> event = new java.util.HashMap<>();
        event.put("paymentId", payment.getId());
        event.put("accountNumber", payment.getAccountNumber());
        event.put("amount", payment.getAmount());
        event.put("reason", payment.getFailureReason());

        kafkaTemplate.send(
                PAYMENT_FAILED_TOPIC,
                String.valueOf(payment.getId()),
                event
        );
    }

    private Map<String, Object>  extractPaymentData(Map<String, Object> payload){

        Map<String, Object> entity = (Map<String, Object>) payload.get("payload");
        Map<String, Object> paymentWrapper = (Map<String, Object>) entity.get("payment");

        return (Map<String, Object>) paymentWrapper.get("entity");

    }

}
