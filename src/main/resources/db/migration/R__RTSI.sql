DROP VIEW IF EXISTS "RTSI";
DROP VIEW IF EXISTS "RI.RTSI";
DROP VIEW IF EXISTS "RI.RTSI.10";

CREATE VIEW "RI.RTSI" AS
WITH "RTSI" AS (SELECT *,
                            first_value("open") OVER (
                        PARTITION BY "ticker", "per"
                        ORDER BY "date"
                        ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS "NEXT_DAY_OPEN",
                            AVG("close") OVER
                        (PARTITION BY "ticker", "per"
                        ORDER BY "date"
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS "SMA50",
                            AVG("close") OVER
                        (PARTITION BY "ticker", "per"
                        ORDER BY "date"
                        ROWS BETWEEN 200 PRECEDING AND CURRENT ROW) AS "SMA200",
                            COUNT(*) FILTER ( WHERE "close" - "open" > 0 ) OVER
                        ( PARTITION BY "ticker", "per", date_part('dow', "date")
                        ORDER BY "date"
                        ROWS BETWEEN 200 PRECEDING AND CURRENT ROW) AS "DOW_CLOSE_POSITIVE_COUNT",
                            COUNT(*) FILTER ( WHERE "close" - "open" < 0 ) OVER
                        ( PARTITION BY "ticker", "per", date_part('dow', "date")
                        ORDER BY "date"
                        ROWS BETWEEN 200 PRECEDING AND CURRENT ROW) AS "DOW_CLOSE_NEGATIVE_COUNT",
                            AVG("high" - "low") OVER
                        ( PARTITION BY "ticker", "per", date_part('dow', "date") ) AS "DOW_AVG_SPREAD",
                            AVG("high" - "low") OVER
                        ( PARTITION BY "ticker", "per", date_part('day', "date") ) AS "DAY_AVG_SPREAD",
                            AVG("high" - "low") OVER
                        ( PARTITION BY "ticker", "per", date_part('month', "date") ) AS "MNTH_AVG_SPREAD",
                            first_value("open") OVER (
                        PARTITION BY "ticker", "per", date_part('year', "date"), date_part('month', "date")
                        ORDER BY "date" DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS "MNTH_CLOSE",
                            first_value("open") OVER (
                        PARTITION BY "ticker", "per", date_part('year', "date"), date_part('month', "date")
                        ORDER BY "date"
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS "MNTH_OPEN"
                FROM "prices_imported"
                WHERE "ticker" = 'RI.RTSI' AND "per" = 'D')
SELECT "RTSI"."ticker",
    "RTSI"."per",
    "RTSI"."date",
    "RTSI"."time",
    "RTSI"."open",
    "RTSI"."high",
    "RTSI"."low",
    "RTSI"."close",
    "RTSI"."vol",
    "RTSI"."NEXT_DAY_OPEN",
    "RTSI"."SMA50",
    "RTSI"."SMA200",
    "RTSI"."DOW_CLOSE_POSITIVE_COUNT",
    "RTSI"."DOW_CLOSE_NEGATIVE_COUNT",
    "RTSI"."DOW_AVG_SPREAD",
    "RTSI"."DAY_AVG_SPREAD",
    "RTSI"."MNTH_AVG_SPREAD",
            COUNT("RTSI".*) FILTER ( WHERE "RTSI"."MNTH_CLOSE" - "RTSI"."MNTH_OPEN" > 0) OVER
        (PARTITION BY "RTSI"."ticker", "RTSI"."per", date_part('month', "RTSI"."date")) AS "MNTH_CLOSE_POSITIVE_COUNT",
            COUNT("RTSI".*) FILTER ( WHERE "RTSI"."MNTH_CLOSE" - "RTSI"."MNTH_OPEN" < 0) OVER
        (PARTITION BY "RTSI"."ticker", "RTSI"."per", date_part('month', "RTSI"."date")) AS "MNTH_CLOSE_NEGATIVE_COUNT",
    "USDRUB"."close" - "USDRUB"."open" AS "usdrub_close_open",
    "USDRUB"."vol" AS "usdrub_vol",
    "BRN"."close" - "BRN"."open" AS "brn_close_open",
    "BRN"."vol" AS "brn_vol",
    "SP500"."close" - "SP500"."open" AS "sp500_close_open",
    "SP500"."vol" AS "sp500__vol"
FROM "RTSI"
         INNER JOIN "prices_imported" AS "USDRUB" ON
        "USDRUB"."date" = "RTSI"."date" AND "USDRUB"."time" = "RTSI"."time" AND "USDRUB"."per" = "RTSI"."per" AND
        "USDRUB"."ticker" = 'USDRUB'
         INNER JOIN "prices_imported" AS "BRN" ON
        "BRN"."date" = "RTSI"."date" AND "BRN"."time" = "RTSI"."time" AND "BRN"."per" = "RTSI"."per" AND
        "BRN"."ticker" = 'ICE.BRN'
         INNER JOIN "prices_imported" AS "SP500" ON
        "SP500"."date" = "RTSI"."date" AND "SP500"."time" = "RTSI"."time" AND "SP500"."per" = "RTSI"."per" AND
        "SP500"."ticker" = 'SANDP-500'
ORDER BY "RTSI"."date", "RTSI"."time";

CREATE VIEW "RI.RTSI.10" AS
SELECT *
FROM "prices_imported"
WHERE "ticker" = 'RI.RTSI' AND "per" = '10' AND
    EXISTS(SELECT 1 FROM "RI.RTSI" WHERE "RI.RTSI"."date" = "prices_imported"."date" AND "RI.RTSI"."per" = 'D');
