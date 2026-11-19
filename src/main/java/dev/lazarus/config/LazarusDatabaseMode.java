package dev.lazarus.config;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;

/** Validates the explicitly selected SQL dialect against the connected database. */
public final class LazarusDatabaseMode {
    private final String name;

    private LazarusDatabaseMode(String name) {
        this.name = name;
    }

    public static LazarusDatabaseMode validate(DataSource dataSource, String configuredMode) throws SQLException {
        String mode = configuredMode == null ? "" : configuredMode.trim().toLowerCase(Locale.ROOT);
        if (!"h2".equals(mode) && !"postgresql".equals(mode)) {
            throw new IllegalArgumentException("lazarus.database.mode must be 'postgresql' or 'h2'");
        }

        String product;
        try (Connection connection = dataSource.getConnection()) {
            product = connection.getMetaData().getDatabaseProductName();
        }
        boolean matches = "h2".equals(mode)
                ? "H2".equalsIgnoreCase(product)
                : product.toLowerCase(Locale.ROOT).contains("postgres");
        if (!matches) {
            throw new IllegalStateException("Configured database mode '" + mode
                    + "' does not match connected database '" + product + "'");
        }
        return new LazarusDatabaseMode(mode);
    }

    public String name() {
        return name;
    }
}
