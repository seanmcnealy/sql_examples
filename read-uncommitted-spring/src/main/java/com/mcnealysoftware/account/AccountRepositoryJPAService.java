package com.mcnealysoftware.account;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Service
public class AccountRepositoryJPAService {

    @Autowired
    AccountRepositoryJPA accountRepository;

    @Transactional(readOnly = false, isolation = Isolation.READ_UNCOMMITTED)
    public long createAccount(String name, BigDecimal balance) {
        final var account = new Account();
        account.setName(name);
        account.setBalance(balance);
        accountRepository.save(account);
        return account.getId();
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public BigDecimal getBalance(long accountId) {
        return accountRepository.findById(accountId).get().getBalance();
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_COMMITTED)
    public BigDecimal getBalanceCommitted(long accountId) {
        return accountRepository.findById(accountId).get().getBalance();
    }

    @Transactional(readOnly = false, isolation = Isolation.READ_UNCOMMITTED)
    public void moveAmount(long fromAccountId, long toAccountId, BigDecimal amount) {
        accountRepository.moveAmount(fromAccountId, amount.negate());
        accountRepository.moveAmount(toAccountId, amount);
    }

    @Retryable(maxAttempts = 100, backoff = @Backoff(0))
    @Transactional(readOnly = false, isolation = Isolation.SERIALIZABLE)
    public void moveAmountSerializable(long fromAccountId, long toAccountId, BigDecimal amount) {
        final var fromAccount = accountRepository.findById(fromAccountId).get();
        fromAccount.setBalance(fromAccount.getBalance().subtract(amount));
        final var toAccount = accountRepository.findById(toAccountId).get();
        toAccount.setBalance(toAccount.getBalance().add(amount));
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public BigDecimal getTotalBalances() {
        return accountRepository.getTotalBalances();
    }

    @Retryable(maxAttempts = 100, backoff = @Backoff(0))
    @Transactional(readOnly = true, isolation = Isolation.READ_COMMITTED)
    public BigDecimal getTotalBalancesCommitted() {
        return accountRepository.getTotalBalances();
    }
}
