package ru.m407.stock.manager.services;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;

@Service
public class SourceDataLoader {
  private static final Logger log = LoggerFactory.getLogger(SourceDataLoader.class);

  @Autowired
  DataSource dataSource;

  public void loadData() {
    log.info("Loading csv data STARTED");
    if (dataSource != null) {
      File[] csvFiles = new File("data")
              .listFiles((file, s) -> {
                return s.endsWith(".csv");
              });
      Arrays.stream(csvFiles).forEach(file -> {
        /*
        val name = file.getName
      println("file name : " + name)
      val tableName = name.split('.')(0)
      val rowsInserted = new CopyManager(conn.asInstanceOf[BaseConnection])
        .copyIn(s"COPY $tableName FROM STDIN (DELIMITER '~',FORMAT csv)",
          new BufferedReader(new FileReader(file.getPath)))

      println(s"$rowsInserted row(s) inserted for file $file")
         */
        String name = file.getName();
        String tableName = name.split(".csv")[0]
                .replace(".", "_")
                .toLowerCase();
        log.info("file name: {}", name);
        log.info("table name: {}", tableName);
        try {
          Long rowsCount = new CopyManager((BaseConnection) dataSource.getConnection())
                  .copyIn("COPY " + tableName + " FROM STDIN (DELIMITER '~',FORMAT csv))",
                          new BufferedReader(new FileReader(file.getPath())));
          log.info("{} row(s) inserted for file {}", rowsCount, name);
        } catch (Exception e) {
          log.error("Load failed", e);
        }
      });
      log.info("Loading csv data FINISHED");
    }
  }
}
