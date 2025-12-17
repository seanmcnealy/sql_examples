package com.mcnealysoftware.serializable.account;

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
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@Testcontainers
public class AccountRepositoryJPAServiceTest {

    @Container
    private static final MySQLContainer database = new MySQLContainer(DockerImageName.parse("mysql:8.0.36"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", database::getJdbcUrl);
        registry.add("spring.datasource.username", database::getUsername);
        registry.add("spring.datasource.password", database::getPassword);
        registry.add("spring.datasource.driver-class-name", database::getDriverClassName);
    }

    @Autowired
    AccountRepositoryJPAService dao;

    @BeforeEach
    void setUp() {
        final var config = new HikariConfig();
        config.setJdbcUrl(database.getJdbcUrl());
        config.setUsername(database.getUsername());
        config.setPassword(database.getPassword());
        config.setDriverClassName(database.getDriverClassName());
        try(var datasource = new HikariDataSource(config)) {
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
        try(var datasource = new HikariDataSource(config)) {
            try(var connection = datasource.getConnection()){
                connection.createStatement().execute("DROP TABLE account;");
                connection.createStatement().execute("DROP TABLE flyway_schema_history;");
            }
        }
    }

    @Test
    void moveAccountTestConcurrentSerializable() throws InterruptedException {
        final var alice = dao.createAccount("Alice", BigDecimal.valueOf(1000L));
        final var bob = dao.createAccount("Bob", BigDecimal.valueOf(2000L));

        final var totals = new LinkedList<BigDecimal>();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 100; i++) {
                executor.submit(() -> dao.moveAmountSerializableLocking(alice, bob, BigDecimal.valueOf(1L)));
                if( i % 10 == 0) {
                    executor.submit(() -> {
                        totals.add(dao.getTotalBalancesCommitted());
                    });
                }
            }

            executor.shutdown();
            executor.awaitTermination(100, TimeUnit.SECONDS);
        }

        assertEquals(900L, dao.getBalance(alice).longValue());
        assertEquals(2100L, dao.getBalance(bob).longValue());
        assertEquals(3000L, dao.getTotalBalances().longValue());
        assertEquals(Optional.of(30000L), totals.stream().reduce(BigDecimal::add).map(BigDecimal::longValue));
    }

    @Test
    void moveAccountTestConcurrentSerializableRetrying() throws InterruptedException {
        final var alice = dao.createAccount("Alice", BigDecimal.valueOf(1000L));
        final var bob = dao.createAccount("Bob", BigDecimal.valueOf(2000L));

        final var totals = new LinkedList<BigDecimal>();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 100; i++) {
                executor.submit(() -> dao.moveAmountSerializableRetrying(alice, bob, BigDecimal.valueOf(1L)));
                if( i % 10 == 0) {
                    executor.submit(() -> {
                        totals.add(dao.getTotalBalancesCommitted());
                    });
                }
            }

            executor.shutdown();
            executor.awaitTermination(100, TimeUnit.SECONDS);
        }

        assertEquals(900L, dao.getBalance(alice).longValue());
        assertEquals(2100L, dao.getBalance(bob).longValue());
        assertEquals(3000L, dao.getTotalBalances().longValue());
        assertEquals(Optional.of(30000L), totals.stream().reduce(BigDecimal::add).map(BigDecimal::longValue));
    }
}
