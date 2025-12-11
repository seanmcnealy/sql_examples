package com.mcnealysoftware.account;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

@EnableRetry
@SpringBootApplication
public class TestApplication {
    static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }
}
