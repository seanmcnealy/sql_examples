package com.mcnealysoftware.account;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.PreparedStatement;

@Repository
public class AccountRepositoryJdbc {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public AccountRepositoryJdbc(DataSource dataSource){
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Transactional(readOnly = false, isolation = Isolation.READ_UNCOMMITTED)
    public long createAccount(String name, BigDecimal balance) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(
                con -> {
                    var ps = con.prepareStatement(
                            "INSERT INTO account (name, balance) VALUES (?, ?);", PreparedStatement.RETURN_GENERATED_KEYS);
                    ps.setString(1, name);
                    ps.setBigDecimal(2, balance);
                    return ps;
                }, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public BigDecimal getBalance(long accountId) {
        return jdbcTemplate.queryForObject("SELECT balance FROM account WHERE id = ?;", BigDecimal.class, accountId);
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_COMMITTED)
    public BigDecimal getBalanceCommitted(long accountId) {
        return jdbcTemplate.queryForObject("SELECT balance FROM account WHERE id = ?;", BigDecimal.class, accountId);
    }

    @Transactional(readOnly = false, isolation = Isolation.READ_UNCOMMITTED)
    public void moveAmount(long fromAccountId, long toAccountId, BigDecimal amount) {
        jdbcTemplate.update("UPDATE account SET balance = balance - ? WHERE id = ?;", amount, fromAccountId);
        jdbcTemplate.update("UPDATE account SET balance = balance + ? WHERE id = ?;", amount, toAccountId);
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public BigDecimal getTotalBalances() {
        return jdbcTemplate.queryForObject("SELECT SUM(balance) FROM account;", BigDecimal.class);
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_COMMITTED)
    public BigDecimal getTotalBalancesCommitted() {
        return jdbcTemplate.queryForObject("SELECT SUM(balance) FROM account;", BigDecimal.class);
    }
}