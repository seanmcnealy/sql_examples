package com.mcnealysoftware.readcommitted.account;

import com.mcnealysoftware.readcommitted.Page;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * DAO for accounts and their balances. Allows for creating accounts, getting balances, and moving money between accounts.
 * @param dataSource Database configuration that creates Connections.
 * @param isolationLevel This one is weird as a parameter, but for this example we want to test different isolation levels.
 */
public record AccountDao(DataSource dataSource, int isolationLevel) {

    /**
     * Creates a new account.
     * @param name Something to identify the account, but not important.
     * @param balance Initial balance.
     * @return com.mcnealysoftware.readcommitted.account.Account ID.
     */
    public long createAccount(String name, BigDecimal balance) {
        try (final Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.setReadOnly(false);
            connection.setTransactionIsolation(isolationLevel);
            final var statement = connection.prepareStatement("INSERT INTO account (name, balance) VALUES (?, ?);", Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, name);
            statement.setBigDecimal(2, balance);
            statement.executeUpdate();
            final var resultSet = statement.getGeneratedKeys();
            resultSet.next();
            final var id = resultSet.getLong(1);
            connection.commit();
            return id;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the balance of an account.
     */
    public BigDecimal getBalance(long accountId) {
        try (final Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.setReadOnly(true);
            connection.setTransactionIsolation(isolationLevel);

            final var statement = connection.prepareStatement("SELECT balance FROM account WHERE id = ?;");
            statement.setLong(1, accountId);
            final var resultSet = statement.executeQuery();
            resultSet.next();
            final var id = resultSet.getBigDecimal(1);

            connection.commit();
            return id;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the total balance of all accounts.
     * NOTE: This function has interesting behavior based on the isolation level.
     */
    public Page<Account> getAccounts(int page, int pageSize) {
        try (final Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.setReadOnly(true);
            connection.setTransactionIsolation(isolationLevel);

            final var statement = connection.prepareStatement("SELECT id, name, balance FROM account LIMIT ? OFFSET ?;");
            statement.setInt(1, pageSize);
            statement.setInt(2, page * pageSize);
            final var resultSet = statement.executeQuery();

            final var accounts = new java.util.ArrayList<Account>();
            while(resultSet.next()){
                final var id = resultSet.getLong(1);
                final var name = resultSet.getString(2);
                final var balance = resultSet.getBigDecimal(3);
                accounts.add(new Account(id, name, balance));
            };

            final var sizeStatement = connection.prepareStatement("SELECT COUNT(*) FROM account;");
            final var sizeResultSet = sizeStatement.executeQuery();
            sizeResultSet.next();
            final var numAccounts = sizeResultSet.getLong(1);

            connection.commit();
            return new Page<>(accounts, numAccounts);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
