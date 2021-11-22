package ru.m407.stock.manager.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;

@Service
@Slf4j
public class HistoryInfo {
  DataSource dataSource;

  public HistoryInfo(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public LocalDate latestStoredDate() {
    try {
      Statement st = dataSource.getConnection().createStatement();
      ResultSet rs = st.executeQuery("select max(ph.date)\n" +
              "from prices_history as ph\n" +
              "where ph.ticker = 'RI.RTSI';");
      if(rs.next()) {
        return rs.getDate(1).toLocalDate().minusDays(1);
      }
    } catch (Exception e) {
      log.error("Data move to history failed");
    }
    return LocalDate.of(2014, 1,1);
  }
}
