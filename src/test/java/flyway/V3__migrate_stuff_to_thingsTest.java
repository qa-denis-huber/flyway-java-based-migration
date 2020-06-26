package flyway;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.migration.Context;
import org.h2.Driver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.nio.file.Files;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class V3__migrate_stuff_to_thingsTest {
    V3__migrate_stuff_to_things sut;

    @BeforeEach
    void setUp() {
        sut = new V3__migrate_stuff_to_things();
    }

    @Test
    void migrate() throws Exception {
        String user = "sa";
        String password = "";
        String tempFile = Files.createTempFile("h2-db-", "").toAbsolutePath().toString();
        // Do not use in memory h2 db
        String jdbcUrl = "jdbc:h2:" + tempFile + ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";

        Driver h2Driver = new Driver();
        Properties info = new Properties();
        info.put("user", user);
        info.put("password", password);
        Connection connection = h2Driver.connect(jdbcUrl, info);
        JdbcTemplate jdbc = new JdbcTemplate(new SingleConnectionDataSource(connection, true));

        Flyway flyway = Flyway.configure()
                .dataSource(jdbcUrl, user, password)
                .locations("classpath:flyway")
                .target("2")
                .load();
        flyway.migrate();

        // Insert test data
        jdbc.update("INSERT INTO stuff (id, item) VALUES(?, ?)", UUID.randomUUID(), "hello world");
        List<V3__migrate_stuff_to_things.StuffV2Entity> stuffV2 = jdbc.query(
                "SELECT * FROM stuff",
                (rs, rowNum) -> new V3__migrate_stuff_to_things.StuffV2Entity(rs.getObject("id").toString(), rs.getString("item"))
        );
        assertEquals(stuffV2.get(0).getItem(), "hello world");

        // when: migrate
        sut.migrate(new ContextV3(null, connection));

        // then: data is in the new table
        List<ThingsV3Entity> thingsV3 = jdbc.query(
                "SELECT * FROM things",
                (rs, rowNum) -> new ThingsV3Entity(rs.getObject("id").toString(), rs.getString("something"))
        );
        assertEquals(thingsV3.get(0).getSomething(), "dlrow olleh");
    }

    @Value
    static class ThingsV3Entity {
        String id;
        String something;
    }

    @AllArgsConstructor
    private static class ContextV3 implements Context {
        private final Configuration configuration;
        private final Connection connection;

        @Override
        public Configuration getConfiguration() {
            return configuration;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }
    }
}
