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
     * @return Account ID.
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
     * Moves an amount from one account to another. Uses one transaction to decrement an account and increment another.
     */
    public void moveAmount(long fromAccountId, long toAccountId, BigDecimal amount) {
        try (final Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.setReadOnly(false);
            connection.setTransactionIsolation(isolationLevel);

            final var statement1 = connection.prepareStatement("UPDATE account SET balance = balance - ? WHERE id = ?");
            statement1.setBigDecimal(1, amount);
            statement1.setLong(2, fromAccountId);
            statement1.executeUpdate();
            final var statement2 = connection.prepareStatement("UPDATE account SET balance = balance + ? WHERE id = ?");
            statement2.setBigDecimal(1, amount);
            statement2.setLong(2, toAccountId);
            statement2.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the total balance of all accounts.
     * NOTE: This function has interesting behavior based on the isolation level.
     */
    public BigDecimal getTotalBalances() {
        try (final Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.setReadOnly(true);
            connection.setTransactionIsolation(isolationLevel);

            final var statement = connection.prepareStatement("SELECT SUM(balance) FROM account;");
            final var resultSet = statement.executeQuery();
            resultSet.next();
            final var id = resultSet.getBigDecimal(1);

            connection.commit();
            return id;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
