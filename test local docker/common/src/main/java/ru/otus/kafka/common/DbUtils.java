package ru.otus.kafka.common;

import java.sql.*;

/**
 * Утилиты для работы с PostgreSQL.
 */
public final class DbUtils {

    private DbUtils() {
        // utility class
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                EnvUtils.getJdbcUrl(),
                EnvUtils.getJdbcUser(),
                EnvUtils.getJdbcPassword()
        );
    }

    public static void execute(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    public static boolean exists(Connection c, String sql, String arg) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, arg);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }
}
