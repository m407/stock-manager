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

  private LocalDate latestStoredDate;
  private ArrayList<TikerMeta> tikers;
  private final String finamBaseUrl = "https://export.finam.ru/export9.out";
  private final String finamDefaultParams = "apply=0&p=8&dtf=1&tmf=3&MSOR=1&mstime=on&mstimever=1&sep=1&sep2=1&datf=1&at=1";

  public RemoteDataLoader(HistoryInfo historyInfo) {
    this.latestStoredDate = historyInfo.latestStoredDate();
    tikers = new ArrayList();
    tikers.add(new TikerMeta(
            "market=91&em=420445",
            "RI.RTSI"
    ));
    tikers.add(new TikerMeta(
            "market=5&em=901",
            "USDRUB"
    ));
    tikers.add(new TikerMeta(
            "market=31&em=19473",
            "ICE.BRN"
    ));
    tikers.add(new TikerMeta(
            "market=6&em=90",
            "SANDP-500"
            ));
  }

  private URL createTikerUrl(TikerMeta tikerMeta) {
    LocalDate dateFrom = latestStoredDate;
    LocalDate dateTo = LocalDate.now();
    String url = String.format("%s?%s&%s&cn=%s&df=%d&mf=%d&yf=%d&dt=%d&mt=%d&yt=%d",
            finamBaseUrl,
            finamDefaultParams,
            tikerMeta.url,
            tikerMeta.tiker,
            dateFrom.getDayOfMonth(), dateFrom.getMonthValue()-1, dateFrom.getYear(),
            dateTo.getDayOfMonth(), dateTo.getMonthValue()-1, dateTo.getYear());
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
