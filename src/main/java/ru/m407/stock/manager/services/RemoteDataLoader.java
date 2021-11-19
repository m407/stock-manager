package ru.m407.stock.manager.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.ArrayList;

@Service
@Slf4j
public class RemoteDataLoader {

  class TikerMeta {
    protected final String tiker;
    protected final String url;

    public TikerMeta(String url, String tiker) {
      this.url = url;
      this.tiker = tiker;
    }
  }

  private int latestStoredYear;
  private ArrayList<TikerMeta> tikers;
  private final String finamBaseUrl = "https://export.finam.ru/export9.out";
  private final String finamDefaultParams = "apply=0&p=8&dtf=1&tmf=3&MSOR=1&mstime=on&mstimever=1&sep=1&sep2=1&datf=1&at=1";

  public RemoteDataLoader(HistoryInfo historyInfo) {
    this.latestStoredYear = historyInfo.latestStoredYear();
    tikers = new ArrayList();
    tikers.add(new TikerMeta(
            "RI.RTSI",
            "market=91&em=420445"
    ));
    tikers.add(new TikerMeta(
            "USDRUB",
            "market=5&em=901"
    ));
    tikers.add(new TikerMeta(
            "ICE.BRN",
            "market=31&em=19473"
    ));
    tikers.add(new TikerMeta(
            "SANDP-500",
            "market=6&em=90"
    ));
  }

  private URL createTikerUrl(TikerMeta tikerMeta) {
    LocalDate dateFrom = LocalDate.of(latestStoredYear, 1, 1);
    LocalDate dateTo;
    if (latestStoredYear == LocalDate.now().getYear()) {
      dateTo = LocalDate.now();
    } else {
      dateTo = LocalDate.of(latestStoredYear, 12, 31);
    }
    String url = String.format("%s?%s&%s&cn=%s&df=%d&mf=%d&yf=%d&dt=%d&mt=%d&yt=%d",
            finamBaseUrl,
            finamDefaultParams,
            tikerMeta.tiker,
            tikerMeta.url,
            dateFrom.getDayOfMonth(), dateFrom.getMonthValue(), dateFrom.getYear(),
            dateTo.getDayOfMonth(), dateTo.getMonthValue(), dateTo.getYear());
    try {
      return new URL(url);
    } catch (Exception e) {
      return null;
    }
  }

  public void download() {
    tikers.forEach(tikerMeta -> {
      String fileName = "data/" + tikerMeta.tiker + ".csv";
      try {
        InputStream in = this.createTikerUrl(tikerMeta).openStream();
        Files.copy(in, Paths.get(fileName), StandardCopyOption.REPLACE_EXISTING);
      } catch (Exception e) {
        log.error("Could not load " + fileName, e);
      }
    });
  }
}
