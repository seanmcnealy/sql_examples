package com.mcnealysoftware.readcommitted.account;

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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class AccountDaoTest {
    @FunctionalInterface
    public interface CheckedConsumer<T> {
        void accept(T t) throws SQLException, InterruptedException;
    }

    private void setup(CheckedConsumer<DataSource> f) throws SQLException, InterruptedException {
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
    void createAccountTest() throws SQLException, InterruptedException {
        setup(connection -> {
            final var dao = new AccountDao(connection, Connection.TRANSACTION_READ_UNCOMMITTED);

            final var alice = dao.createAccount("Alice", BigDecimal.valueOf(1000L));
            final var bob = dao.createAccount("Bob", BigDecimal.valueOf(2000L));

            assertEquals(1, alice);
            assertEquals(2, bob);
        });
    }

    @Test
    void createAccountTestConcurrentReadCommitted() throws SQLException, InterruptedException {
        setup(connection -> {
            final var dao = new AccountDao(connection, Connection.TRANSACTION_READ_COMMITTED);

            final var names = List.of("Alice", "Bob", "Charlie", "David", "Eddie", "Frank", "George", "Harry", "Ivan", "John");

            final var totals = new LinkedList<List<Long>>();

            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < 100; i++) {
                    final int finalI = i;
                    executor.submit(() -> dao.createAccount(names.get(finalI % names.size()), BigDecimal.valueOf(1000L)));
                    if( i % 10 == 0) {
                        executor.submit(() -> {
                            final var accounts = dao.getAccounts(0, 100);
                            totals.add(List.of((long) accounts.items().size(), accounts.total()));
                        });
                    }
                }

                executor.shutdown();
                executor.awaitTermination(100, TimeUnit.SECONDS);
            }

            final var accounts = dao.getAccounts(0, 200);
            assertEquals(100, accounts.items().size());

            var allEqual = true;
            for(int i = 1; i < totals.size(); i++) {
                if (totals.get(i).get(0) != totals.get(i).get(1)) {
                    allEqual = false;
                    break;
                }
            }
            assertFalse(allEqual);
        });
    }

    @Test
    void createAccountTestConcurrentRepeatableRead() throws SQLException, InterruptedException {
        setup(connection -> {
            final var dao = new AccountDao(connection, Connection.TRANSACTION_REPEATABLE_READ);

            final var names = List.of("Alice", "Bob", "Charlie", "David", "Eddie", "Frank", "George", "Harry", "Ivan", "John");

            final var totals = new LinkedList<List<Long>>();

            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < 100; i++) {
                    final int finalI = i;
                    executor.submit(() -> dao.createAccount(names.get(finalI % names.size()), BigDecimal.valueOf(1000L)));
                    if( i % 10 == 0) {
                        executor.submit(() -> {
                            final var accounts = dao.getAccounts(0, 100);
                            totals.add(List.of((long) accounts.items().size(), accounts.total()));
                        });
                    }
                }

                executor.shutdown();
                executor.awaitTermination(100, TimeUnit.SECONDS);
            }

            final var accounts = dao.getAccounts(0, 200);
            assertEquals(100, accounts.items().size());

            totals.forEach(t -> assertEquals(t.get(0), t.get(1)));
        });
    }
}
