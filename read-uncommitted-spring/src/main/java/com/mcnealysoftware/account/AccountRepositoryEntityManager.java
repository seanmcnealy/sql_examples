package com.mcnealysoftware.account;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Repository
public class AccountRepositoryEntityManager {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    public AccountRepositoryEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Transactional(readOnly = false, isolation = Isolation.READ_UNCOMMITTED)
    public long createAccount(String name, BigDecimal balance) {
        final var account = new Account();
        account.setName(name);
        account.setBalance(balance);
        entityManager.persist(account);
        entityManager.flush();
        return account.getId();
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public BigDecimal getBalance(long accountId) {
        return entityManager.find(Account.class, accountId).getBalance();
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_COMMITTED)
    public BigDecimal getBalanceCommitted(long accountId) {
        return entityManager.find(Account.class, accountId).getBalance();
    }

    @Transactional(readOnly = false, isolation = Isolation.READ_UNCOMMITTED)
    public void moveAmount(long fromAccountId, long toAccountId, BigDecimal amount) {
        final var fromQuery = entityManager.createQuery("UPDATE Account SET balance = balance - ?1 WHERE id = ?2");
        fromQuery.setParameter(1, amount);
        fromQuery.setParameter(2, fromAccountId);
        fromQuery.executeUpdate();
        final var toQuery = entityManager.createQuery("UPDATE Account SET balance = balance + ?1 WHERE id = ?2");
        toQuery.setParameter(1, amount);
        toQuery.setParameter(2, toAccountId);
        toQuery.executeUpdate();
    }

    @Retryable(maxAttempts = 100, backoff = @Backoff(0))
    @Transactional(readOnly = false, isolation = Isolation.SERIALIZABLE)
    public void moveAmountSerializable(long fromAccountId, long toAccountId, BigDecimal amount) {
        final var fromAccount = entityManager.find(Account.class, fromAccountId);
        fromAccount.setBalance(fromAccount.getBalance().subtract(amount));
        final var toAccount = entityManager.find(Account.class, toAccountId);
        toAccount.setBalance(toAccount.getBalance().add(amount));
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public BigDecimal getTotalBalances() {
        final var query = entityManager.createQuery("SELECT SUM(balance) FROM Account", BigDecimal.class);
        return query.getSingleResult();
    }

    @Retryable(maxAttempts = 100, backoff = @Backoff(0))
    @Transactional(readOnly = true, isolation = Isolation.READ_COMMITTED)
    public BigDecimal getTotalBalancesCommitted() {
        final var query = entityManager.createQuery("SELECT SUM(balance) FROM Account", BigDecimal.class);
        return query.getSingleResult();
    }
}