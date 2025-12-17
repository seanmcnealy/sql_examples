package com.mcnealysoftware.serializable.account;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.Optional;

@Repository
public interface AccountRepositoryJPA extends CrudRepository<Account, Long> {
    @Modifying
    @Query("UPDATE Account SET balance = balance + :amount WHERE id = :accountId")
    void moveAmount(long accountId, BigDecimal amount);

    @Query("SELECT SUM(a.balance) FROM Account AS a")
    BigDecimal getTotalBalances();

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT a FROM Account AS a WHERE a.id = :id")
    Optional<Account> findByIdWithPessimisticWriteLock(long id);
}
