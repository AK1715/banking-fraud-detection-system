package com.banking.transactionservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransferRequest {
    @NotBlank(message = "Sender Account Number is Required")
    private String senderAccountNumber;

    @NotBlank(message = "Receiver Account Number is Required")
    private String receiverAccountNumber;

    @NotBlank(message = "Amount is Required")
    @Positive(message = "Amount must be positive")
    private BigDecimal amount;

    private String description;

}
