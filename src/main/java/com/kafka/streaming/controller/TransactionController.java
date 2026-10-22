package com.kafka.streaming.controller;

import com.kafka.streaming.service.TransactionService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("api/v1")
public class TransactionController {

    private final TransactionService transactionService;

    @GetMapping("/transaction/process")
    public String processTransaction() {
        return transactionService.processTransactions();
    }
}
