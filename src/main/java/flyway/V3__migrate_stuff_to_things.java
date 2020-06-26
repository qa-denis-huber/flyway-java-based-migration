package flyway;

import lombok.Value;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class V3__migrate_stuff_to_things extends BaseJavaMigration {
    @Override
    public void migrate(Context context) throws Exception {
        JdbcTemplate jdbc = new JdbcTemplate(new SingleConnectionDataSource(context.getConnection(), true));

        List<StuffV2Entity> stuffList = jdbc.query("SELECT * FROM stuff", (resultSet, num) -> new StuffV2Entity(resultSet.getString("id"), resultSet.getString("item")));

        jdbc.batchUpdate("INSERT INTO things (id, something) VALUES(?, ?)", new BatchSetterV3(stuffList));
    }

    @Value
    static class StuffV2Entity {
        String id;
        String item;
    }

    @Value
    static class BatchSetterV3 implements BatchPreparedStatementSetter {
        List<StuffV2Entity> entities;

        @Override
        public void setValues(PreparedStatement ps, int i) throws SQLException {
            StuffV2Entity entity = entities.get(i);
            ps.setString(1, entity.id);
            ps.setString(2, reverseString(entity.item));
        }

        @Override
        public int getBatchSize() {
            return entities.size();
        }
    }

    private static String reverseString(String str) {
        StringBuilder sb = new StringBuilder(str);
        sb.reverse();
        return sb.toString();
    }
}
