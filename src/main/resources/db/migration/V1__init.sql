create table prices_imported
(
    ticker  text             not null,
    per     text             not null,
    "date"  DATE             NOT NULL,
    "time"  TIME             NOT NULL,
    "open"  DOUBLE PRECISION NOT NULL,
    "high"  DOUBLE PRECISION NOT NULL,
    "low"   DOUBLE PRECISION NOT NULL,
    "close" DOUBLE PRECISION NOT NULL,
    "vol"   BIGINT           NOT NULL
);

CREATE TABLE "prices_history" (
    "ticker" TEXT             NOT NULL,
    "per"    TEXT             NOT NULL,
    "date"   DATE             NOT NULL,
    "time"   TIME             NOT NULL,
    "open"   DOUBLE PRECISION NOT NULL,
    "high"   DOUBLE PRECISION NOT NULL,
    "low"    DOUBLE PRECISION NOT NULL,
    "close"  DOUBLE PRECISION NOT NULL,
    "vol"    BIGINT           NOT NULL,
    CONSTRAINT "prices_history_ticker_per_date_time_pk"
        PRIMARY KEY ("ticker", "per", "date", "time")
);
