package com.mcnealysoftware.readcommitted.account;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
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
    public Page<Account> getAccounts(long page, long pageSize) {
        final var query = entityManager.createQuery("SELECT a FROM Account a", Account.class);
        query.setFirstResult((int) (page * pageSize));
        query.setMaxResults((int) pageSize);
        final var accounts = query.getResultList();

        final var totalAccounts = entityManager.createQuery("SELECT COUNT(*) FROM Account", Long.class).getSingleResult();

        return new PageImpl<>(accounts, Pageable.ofSize((int) 1), totalAccounts);
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Page<Account> getAccountsRepeatableRead(long page, long pageSize) {
        final var query = entityManager.createQuery("SELECT a FROM Account a", Account.class);
        query.setFirstResult((int) (page * pageSize));
        query.setMaxResults((int) pageSize);
        final var accounts = query.getResultList();

        final var totalAccounts = entityManager.createQuery("SELECT COUNT(*) FROM Account", Long.class).getSingleResult();

        return new PageImpl<>(accounts, Pageable.ofSize((int) 1), totalAccounts);
    }
}