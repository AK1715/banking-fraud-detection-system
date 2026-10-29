package com.banking.frauddedecationservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients(basePackages = "com.banking.frauddedecationservice.client")
public class FraudDedecationServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(FraudDedecationServiceApplication.class, args);
	}

}
