package syndeticlogic.catena.performance;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class TrialResultsJdbcDao {
    private static final String create = "create table trial_results(";
    private static final String insert = "insert into trial_results(duration) values(?)";
    private final RowMapper<TrialResult> rowMapper;
    private final JdbcTemplate jdbcTemplate;
    // derby driverclassname == org.apache.derby.jdbc.EmbeddedDriver
    // derby jdbcUrl == jdbc:derby://localhost:21529/tmp/catena/trial_results;create=true
    // postgres driverClassName == org.postgresql.Driver
    // postres jdbcUrl == jdbc:postgresql://localhost:5432/catena?user=james&password=password
    // sqlite driverClassName == org.sqlite.JDBC
    // sqlite jdburl == jdbc:sqlite:sample.db?user=james&password=password
    public TrialResultsJdbcDao(String driverClassName, String jdbcUrl) {
        DriverManagerDataSource source = new DriverManagerDataSource();
        source.setDriverClassName(driverClassName);
        source.setUrl(jdbcUrl);
        jdbcTemplate = new JdbcTemplate(source);
        rowMapper = new TrialResultMapper();
    }
    
    public void insert(TrialResult result) {
        jdbcTemplate.update(insert, result.getDuration());
    }
    
    public List<TrialResult> adHocQuery(String sql, Map<String, Object> args) {
        return jdbcTemplate.query(sql, rowMapper, args);
    }
    
    private class TrialResultMapper implements RowMapper<TrialResult> {
        @Override
        public TrialResult mapRow(ResultSet rs, int rowNum) throws SQLException {
            TrialResult t = new TrialResult();
            t.setDuration(rs.getLong("duration"));
            return t;
        }
    }

}
