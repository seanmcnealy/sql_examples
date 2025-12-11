package com.mcnealysoftware.account;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;

@Repository
public interface AccountRepositoryJPA extends CrudRepository<Account, Long> {
    @Modifying
    @Query("UPDATE Account SET balance = balance + :amount WHERE id = :accountId")
    void moveAmount(long accountId, BigDecimal amount);

    @Query("SELECT SUM(a.balance) FROM Account AS a")
    BigDecimal getTotalBalances();
}
