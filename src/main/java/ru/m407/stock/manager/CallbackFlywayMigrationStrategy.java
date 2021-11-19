package ru.m407.stock.manager;

import org.flywaydb.core.Flyway;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.stereotype.Component;
import ru.m407.stock.manager.services.SourceDataLoader;

@Component
public class CallbackFlywayMigrationStrategy implements FlywayMigrationStrategy {

  SourceDataLoader sourceDataLoader;
  public CallbackFlywayMigrationStrategy(SourceDataLoader sourceDataLoader) {
    this.sourceDataLoader = sourceDataLoader;
  }

  @Override
  public void migrate(Flyway flyway) {
    flyway.migrate();
    sourceDataLoader.loadData();
  }

}
