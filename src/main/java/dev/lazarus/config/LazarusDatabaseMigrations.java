package dev.lazarus.config;

import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.SQLException;

/** Runs the versioned shared and database-specific SQL migration locations. */
public final class LazarusDatabaseMigrations {
    private LazarusDatabaseMigrations() {}

    public static void migrate(DataSource dataSource, String databaseMode) throws SQLException {
        LazarusDatabaseMode.validate(dataSource, databaseMode);
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration/common", "classpath:db/migration/" + databaseMode)
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load()
                .migrate();
    }
}
