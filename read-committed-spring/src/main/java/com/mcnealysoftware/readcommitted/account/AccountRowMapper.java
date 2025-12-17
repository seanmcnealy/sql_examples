package com.mcnealysoftware.readcommitted.account;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AccountRowMapper implements RowMapper<Account> {
    @Override
    public Account mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new Account(
                rs.getLong("id"),
                rs.getString("name"),
                rs.getBigDecimal("balance")
        );
    }
}
