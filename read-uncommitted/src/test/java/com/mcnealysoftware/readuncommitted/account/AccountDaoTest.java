package com.mcnealysoftware.readuncommitted.account;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class AccountDaoTest {
    @FunctionalInterface
    public interface CheckedConsumer<T> {
        void accept(T t) throws SQLException;
    }

    private void setup(CheckedConsumer<DataSource> f) throws SQLException {
        try(final var mysql = new MySQLContainer(DockerImageName.parse("mysql:8.0.36"))) {
            mysql.start();
            final var config = new HikariConfig();
            config.setJdbcUrl(mysql.getJdbcUrl());
            config.setUsername(mysql.getUsername());
            config.setPassword(mysql.getPassword());
            config.setDriverClassName(mysql.getDriverClassName());
            try(var datasource = new HikariDataSource(config)) {
                final var flyway = Flyway.configure()
                        .dataSource(datasource).locations("classpath:schema").load();
                flyway.migrate();
                f.accept(datasource);
            }
        }
    }

    @Test
    void createAccountTest() throws SQLException {
        setup(connection -> {
            final var dao = new AccountDao(connection, Connection.TRANSACTION_READ_UNCOMMITTED);

            final var alice = dao.createAccount("Alice", BigDecimal.valueOf(1000L));
            final var bob = dao.createAccount("Bob", BigDecimal.valueOf(2000L));

            assertEquals(1, alice);
            assertEquals(2, bob);
        });
    }

    @Test
    void moveAccountTest() throws SQLException {
        setup(connection -> {
            final var dao = new AccountDao(connection, Connection.TRANSACTION_READ_UNCOMMITTED);

            final var alice = dao.createAccount("Alice", BigDecimal.valueOf(1000L));
            final var bob = dao.createAccount("Bob", BigDecimal.valueOf(2000L));

            dao.moveAmount(alice, bob, BigDecimal.valueOf(100L));

            assertEquals(900L, dao.getBalance(alice).longValue());
            assertEquals( 2100L, dao.getBalance(bob).longValue());
            assertEquals(3000L, dao.getTotalBalances().longValue());
        });
    }

    @Test
    void moveAccountTestConcurrentReadUncommitted() throws SQLException {
        setup(connection -> {
            final var dao = new AccountDao(connection, Connection.TRANSACTION_READ_UNCOMMITTED);

            final var alice = dao.createAccount("Alice", BigDecimal.valueOf(1000L));
            final var bob = dao.createAccount("Bob", BigDecimal.valueOf(2000L));

            final var totals = new LinkedList<BigDecimal>();

            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < 100; i++) {
                    executor.submit(() -> dao.moveAmount(alice, bob, BigDecimal.valueOf(1L)));
                    if( i % 10 == 0) {
                        executor.submit(() -> {
                            totals.add(dao.getTotalBalances());
                        });
                    }
                }

                executor.shutdown();
            }

            assertEquals(900L, dao.getBalance(alice).longValue());
            assertEquals(2100L, dao.getBalance(bob).longValue());
            assertEquals(3000L, dao.getTotalBalances().longValue());
            assertNotEquals(Optional.of(30000L), totals.stream().reduce(BigDecimal::add).map(BigDecimal::longValue));
        });
    }

    @Test
    void moveAccountTestConcurrentReadCommitted() throws SQLException {
        setup(connection -> {
            final var dao = new AccountDao(connection, Connection.TRANSACTION_READ_COMMITTED);

            final var alice = dao.createAccount("Alice", BigDecimal.valueOf(1000L));
            final var bob = dao.createAccount("Bob", BigDecimal.valueOf(2000L));

            final var totals = new LinkedList<BigDecimal>();

            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < 100; i++) {
                    executor.submit(() -> dao.moveAmount(alice, bob, BigDecimal.valueOf(1L)));
                    if( i % 10 == 0) {
                        executor.submit(() -> {
                            totals.add(dao.getTotalBalances());
                        });
                    }
                }

                executor.shutdown();
            }

            assertEquals(900L, dao.getBalance(alice).longValue());
            assertEquals(2100L, dao.getBalance(bob).longValue());
            assertEquals(3000L, dao.getTotalBalances().longValue());
            assertEquals(Optional.of(30000L), totals.stream().reduce(BigDecimal::add).map(BigDecimal::longValue));
        });
    }
}
