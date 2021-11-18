package ru.m407.stock.manager.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

@Service
@Slf4j
public class RemoteDataLoader {
  public void download() {
    try {
      String url = "https://export.finam.ru/export9.out?market=31&em=19473&code=ICE.BRN&apply=0&df=31&mf=2&yf=2014&from=31.03.2014&dt=12&mt=10&yt=2021&to=12.11.2021&p=8&f=ICE.BRN_210331_211112&e=.csv&cn=ICE.BRN&dtf=1&tmf=3&MSOR=1&mstime=on&mstimever=1&sep=1&sep2=1&datf=1&at=1";
      InputStream in = new URL(url).openStream();
      Files.copy(in, Paths.get("data/ICE.BRN_210331_211112.csv"), StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      log.error("Could not load data/CE.BRN_210331_211112.csv", e);
    }
  }
}
