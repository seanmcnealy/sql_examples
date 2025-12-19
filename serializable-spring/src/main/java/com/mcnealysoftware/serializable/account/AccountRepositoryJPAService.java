package com.mcnealysoftware.serializable.account;

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

    @Transactional(readOnly = true, isolation = Isolation.READ_COMMITTED)
    public BigDecimal getBalance(long accountId) {
        return accountRepository.findById(accountId).get().getBalance();
    }

    @Transactional(readOnly = false, isolation = Isolation.READ_COMMITTED)
    public void moveAmount(long fromAccountId, long toAccountId, BigDecimal amount) {
        accountRepository.moveAmount(fromAccountId, amount.negate());
        accountRepository.moveAmount(toAccountId, amount);
    }

    /**
     * This is a WRONG example. But it works based on just massively retrying deadlocked transactions.
     */
    @Retryable(maxAttempts = 100, backoff = @Backoff(0))
    @Transactional(readOnly = false, isolation = Isolation.SERIALIZABLE)
    public void moveAmountSerializableRetrying(long fromAccountId, long toAccountId, BigDecimal amount) {
        final var fromAccount = accountRepository.findById(fromAccountId).get();
        fromAccount.setBalance(fromAccount.getBalance().subtract(amount));
        final var toAccount = accountRepository.findById(toAccountId).get();
        toAccount.setBalance(toAccount.getBalance().add(amount));
    }

    /**
     * Not the easiest way to increase or decrease values, but still an example for codes that may actually
     * require reading, processing, and writing to the database. This required a special call to the repository
     * to
     */
    @Transactional(readOnly = false, isolation = Isolation.SERIALIZABLE)
    public void moveAmountSerializableLocking(long fromAccountId, long toAccountId, BigDecimal amount) {
        if (fromAccountId <= toAccountId) {
            final var fromAccount = accountRepository.findByIdWithPessimisticWriteLock(fromAccountId).get();
            fromAccount.setBalance(fromAccount.getBalance().subtract(amount));
            final var toAccount = accountRepository.findByIdWithPessimisticWriteLock(toAccountId).get();
            toAccount.setBalance(toAccount.getBalance().add(amount));
        } else {
            moveAmountSerializableLocking(toAccountId, fromAccountId, amount.negate());
        }
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
