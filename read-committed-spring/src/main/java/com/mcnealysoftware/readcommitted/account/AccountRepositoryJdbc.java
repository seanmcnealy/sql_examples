package com.mcnealysoftware.readcommitted.account;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.data.domain.Page;

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

    @Transactional(readOnly = true, isolation = Isolation.READ_COMMITTED)
    public Page<Account> getAccounts(long page, long pageSize) {
        final var accounts = jdbcTemplate.query("SELECT id, name, balance FROM account LIMIT ? OFFSET ?;",
                new AccountRowMapper(),
                pageSize,
                page * pageSize
        );
        final var total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM account;", Long.class);
        return new PageImpl<>(accounts, Pageable.ofSize((int) 1), total);
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Page<Account> getAccountsRepeatableRead(long page, long pageSize) {
        final var accounts = jdbcTemplate.query("SELECT id, name, balance FROM account LIMIT ? OFFSET ?;",
                new AccountRowMapper(),
                pageSize,
                page * pageSize
        );
        final var total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM account;", Long.class);
        return new PageImpl<>(accounts, Pageable.ofSize((int) 1), total);
    }
}