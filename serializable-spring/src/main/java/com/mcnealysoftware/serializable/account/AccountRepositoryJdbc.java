package com.mcnealysoftware.serializable.account;

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
    public AccountRepositoryJdbc(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Transactional(readOnly = false, isolation = Isolation.READ_UNCOMMITTED)
    public long createAccount(String name, BigDecimal balance) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(
                con -> {
                    var ps = con.prepareStatement(
                            "INSERT INTO account (name, balance) VALUES (?, ?)", PreparedStatement.RETURN_GENERATED_KEYS);
                    ps.setString(1, name);
                    ps.setBigDecimal(2, balance);
                    return ps;
                }, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_COMMITTED)
    public BigDecimal getBalance(long accountId) {
        return jdbcTemplate.queryForObject("SELECT balance FROM account WHERE id = ?", BigDecimal.class, accountId);
    }

    /**
     * This is a WRONG example. It will, however, at least throw exceptions to show that it did not succeed.
     * Used to show what the exception looks like.
     */
    @Transactional(readOnly = false, isolation = Isolation.SERIALIZABLE)
    public void moveAmountSerializableDeadlocks(long fromAccountId, long toAccountId, BigDecimal amount) {
        final var fromBalance = jdbcTemplate.queryForObject("SELECT balance FROM account WHERE id = ?", BigDecimal.class, fromAccountId);
        jdbcTemplate.update("UPDATE account SET balance = ? WHERE id = ?", fromBalance.add(amount.negate()), fromAccountId);

        final var toBalance = jdbcTemplate.queryForObject("SELECT balance FROM account WHERE id = ?", BigDecimal.class, toAccountId);
        jdbcTemplate.update("UPDATE account SET balance = ? WHERE id = ?", toBalance.add(amount), toAccountId);
    }

    /**
     * Not the easiest way to increase or decrease values, but still an example for codes that may actually
     * require reading, processing, and writing to the database. Notice the FOR UPDATE clause, which is what's
     * different from the deadlocking version.
     */
    @Transactional(readOnly = false, isolation = Isolation.SERIALIZABLE)
    public void moveAmountSerializable(long fromAccountId, long toAccountId, BigDecimal amount) {
        // requires a deterministic order of locking rows, here use the lower account id
        if (fromAccountId <= toAccountId) {
            final var fromBalance = jdbcTemplate.queryForObject("SELECT balance FROM account WHERE id = ? FOR UPDATE", BigDecimal.class, fromAccountId);
            jdbcTemplate.update("UPDATE account SET balance = ? WHERE id = ?", fromBalance.add(amount.negate()), fromAccountId);

            final var toBalance = jdbcTemplate.queryForObject("SELECT balance FROM account WHERE id = ? FOR UPDATE", BigDecimal.class, toAccountId);
            jdbcTemplate.update("UPDATE account SET balance = ? WHERE id = ?", toBalance.add(amount), toAccountId);
        } else {
            moveAmountSerializable(toAccountId, fromAccountId, amount.negate());
        }
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public BigDecimal getTotalBalances() {
        return jdbcTemplate.queryForObject("SELECT SUM(balance) FROM account", BigDecimal.class);
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_COMMITTED)
    public BigDecimal getTotalBalancesCommitted() {
        return jdbcTemplate.queryForObject("SELECT SUM(balance) FROM account", BigDecimal.class);
    }
}