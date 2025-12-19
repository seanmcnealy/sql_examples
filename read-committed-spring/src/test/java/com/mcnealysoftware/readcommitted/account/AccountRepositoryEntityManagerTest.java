package com.mcnealysoftware.readcommitted.account;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Testcontainers
public class AccountRepositoryEntityManagerTest {

    @Container
    private static final MySQLContainer database = new MySQLContainer(DockerImageName.parse("mysql:8.0.36"));
    @Autowired
    AccountRepositoryEntityManager dao;

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", database::getJdbcUrl);
        registry.add("spring.datasource.username", database::getUsername);
        registry.add("spring.datasource.password", database::getPassword);
        registry.add("spring.datasource.driver-class-name", database::getDriverClassName);
    }

    @BeforeEach
    void setUp() {
        final var config = new HikariConfig();
        config.setJdbcUrl(database.getJdbcUrl());
        config.setUsername(database.getUsername());
        config.setPassword(database.getPassword());
        config.setDriverClassName(database.getDriverClassName());
        try (var datasource = new HikariDataSource(config)) {
            final var flyway = Flyway.configure()
                    .dataSource(datasource).locations("classpath:schema").load();
            flyway.migrate();
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        final var config = new HikariConfig();
        config.setJdbcUrl(database.getJdbcUrl());
        config.setUsername(database.getUsername());
        config.setPassword(database.getPassword());
        config.setDriverClassName(database.getDriverClassName());
        try (var datasource = new HikariDataSource(config)) {
            try (var connection = datasource.getConnection()) {
                connection.createStatement().execute("DROP TABLE account");
                connection.createStatement().execute("DROP TABLE flyway_schema_history");
            }
        }
    }

    @Test
    void createAccountTest() {
        final var alice = dao.createAccount("Alice", BigDecimal.valueOf(1000L));
        final var bob = dao.createAccount("Bob", BigDecimal.valueOf(2000L));

        assertEquals(1, alice);
        assertEquals(2, bob);
    }

    @Test
    void createAccountTestConcurrentReadCommitted() throws InterruptedException, ExecutionException {
        final var names = List.of("Alice", "Bob", "Charlie", "David", "Eddie", "Frank", "George", "Harry", "Ivan", "John");

        final var totals = new LinkedList<Future<List<Long>>>();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 100; i++) {
                final int finalI = i;
                executor.submit(() -> dao.createAccount(names.get(finalI % names.size()), BigDecimal.valueOf(1000L)));
                if (i % 10 == 0) {
                    totals.add(executor.submit(() -> {
                        final var accounts = dao.getAccounts(0, 100);
                        return List.of(accounts.get().count(), accounts.getTotalElements());
                    }));
                }
            }

            executor.shutdown();
            executor.awaitTermination(100, TimeUnit.SECONDS);
        }

        final var accounts = dao.getAccounts(0, 200);
        assertEquals(100, accounts.get().count());

        var allEqual = true;
        for (int i = 1; i < totals.size(); i++) {
            if (totals.get(i).get().get(0) != totals.get(i).get().get(1)) {
                allEqual = false;
                break;
            }
        }
        assertFalse(allEqual);
    }

    @Test
    void createAccountTestConcurrentRepeatableRead() throws InterruptedException, ExecutionException {
        final var names = List.of("Alice", "Bob", "Charlie", "David", "Eddie", "Frank", "George", "Harry", "Ivan", "John");

        final var totals = new LinkedList<Future<List<Long>>>();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 100; i++) {
                final int finalI = i;
                executor.submit(() -> dao.createAccount(names.get(finalI % names.size()), BigDecimal.valueOf(1000L)));
                if (i % 10 == 0) {
                    totals.add(executor.submit(() -> {
                        final var accounts = dao.getAccountsRepeatableRead(0, 100);
                        return List.of(accounts.get().count(), accounts.getTotalElements());
                    }));
                }
            }

            executor.shutdown();
            executor.awaitTermination(100, TimeUnit.SECONDS);
        }

        final var accounts = dao.getAccounts(0, 200);
        assertEquals(100, accounts.get().count());

        for (Future<List<Long>> t : totals) {
            assertEquals(t.get().get(0), t.get().get(1));
        }
    }
}
