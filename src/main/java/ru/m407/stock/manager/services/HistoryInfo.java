package ru.m407.stock.manager.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.Statement;

@Service
@Slf4j
public class HistoryInfo {
  DataSource dataSource;

  public HistoryInfo(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public int latestStoredYear() {
    try {
      Statement st = dataSource.getConnection().createStatement();
      ResultSet rs = st.executeQuery("select max(ph.date)\n" +
              "from prices_history as ph\n" +
              "where ph.ticker = 'RI.RTSI';");
      if(rs.next()) {
        return rs.getDate(0).toLocalDate().getYear();
      }
    } catch (Exception e) {
      log.error("Data move to history failed");
    }
    return 2014;
  }
}
