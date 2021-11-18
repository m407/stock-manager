package ru.m407.stock.manager.services;

import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Statement;
import java.util.Arrays;

@Service
@Slf4j
public class SourceDataLoader {
  DataSource dataSource;
  RemoteDataLoader remoteDataLoader;

  public SourceDataLoader(DataSource dataSource, RemoteDataLoader remoteDataLoader) {
    this.dataSource = dataSource;
    this.remoteDataLoader = remoteDataLoader;
  }

  private void before() {
    if (dataSource != null) {
      try {
        Statement st = dataSource.getConnection().createStatement();
        st.execute("TRUNCATE TABLE prices_imported;");
        st.execute("DELETE FROM prices_history WHERE close = 0 AND low = 0 AND open = 0 AND high = 0;");
      } catch (Exception e) {
        log.error("TRUNCATE failed", e);
      }
    }
    remoteDataLoader.download();
  }

  private void after() {
    try {
      Statement st = dataSource.getConnection().createStatement();
      st.execute("INSERT INTO prices_history\n" +
              "SELECT DISTINCT ON (pi.ticker,pi.per, pi.date,pi.time) pi.*\n" +
              "FROM prices_imported pi\n" +
              "         LEFT JOIN prices_history ph\n" +
              "ON pi.ticker = ph.ticker AND ph.per = pi.per AND ph.date = pi.date AND\n" +
              "   ph.time = pi.time\n" +
              "WHERE ph.ticker IS NULL;\n");
    } catch (Exception e) {
      log.error("Data move to history failed");
    }
  }

  public void loadData() {
    log.info("Loading csv data STARTED");
    before();
    if (dataSource != null) {
      File[] csvFiles = new File("data")
              .listFiles((file, s) -> {
                return s.endsWith(".csv");
              });
      Arrays.stream(csvFiles).forEach(file -> {
        String name = file.getName();
        log.info("file name: {}", name);
        try {
          if (dataSource.getConnection().isWrapperFor(PGConnection.class)) {
            PGConnection pgConnection = dataSource.getConnection().unwrap(PGConnection.class);
            Long rowsCount = new CopyManager((BaseConnection) pgConnection)
                    .copyIn("COPY prices_imported FROM STDIN (DELIMITER ',', HEADER true, FORMAT csv)",
                            new BufferedReader(new FileReader(file.getPath())));
            log.info("{} row(s) inserted for file {}", rowsCount, name);
          }
        } catch (Exception e) {
          log.error("Load failed", e);
        }
      });
    }
    after();
    log.info("Loading csv data FINISHED");
  }
}
