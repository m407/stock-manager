CREATE VIEW "RTSI" AS
WITH "RTSI" AS (SELECT *,
                    AVG("close") OVER
                    (PARTITION BY "ticker", "per"
                        ORDER BY "date"
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS "SMA7",
                    AVG("close") OVER
                    (PARTITION BY "ticker", "per"
                        ORDER BY "date"
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS "SMA20",
                    AVG("close") OVER
                    (PARTITION BY "ticker", "per"
                        ORDER BY "date"
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS "SMA30",
                    AVG("close") OVER
                    (PARTITION BY "ticker", "per"
                        ORDER BY "date"
                        ROWS BETWEEN 200 PRECEDING AND CURRENT ROW) AS "SMA200",
                    AVG("close") OVER
                    (PARTITION BY "ticker", "per"
                            ORDER BY "date"
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) +
                                STDDEV_SAMP("close") OVER
                                    (PARTITION BY "ticker", "per"
                                    ORDER BY "date"
                                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * 2 AS "Upper_Bollinger_Band",
                    AVG("close") OVER
                    (PARTITION BY "ticker", "per"
                    ORDER BY "date"
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) -
                        STDDEV_SAMP("close") OVER
                            (PARTITION BY "ticker", "per"
                            ORDER BY "date"
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * 2 AS "Lower_Bollinger_Band"
                FROM "prices_imported"
                WHERE "ticker" = 'RI.RTSI'),
    "RTSI_HIST_DAY" AS (
        SELECT "ticker", "per", date_part('day', "date") AS "date_part",
            AVG("high" - "low") AS "DAY_AVG_SPREAD"
        FROM "prices_imported"
        WHERE "ticker" = 'RI.RTSI'
        GROUP BY "ticker", "per", date_part('day', "date")
    ),
    "RTSI_HIST_MNTH" AS (
        SELECT "ticker", "per", date_part('month', "date") AS "date_part",
            AVG("high" - "low") AS "MNTH_AVG_SPREAD"
        FROM "prices_imported"
        WHERE "ticker" = 'RI.RTSI'
        GROUP BY "ticker", "per", date_part('month', "date")
    )
SELECT "RTSI".*,
    "RTSI_HIST_DAY"."DAY_AVG_SPREAD",
    "RTSI_HIST_MNTH"."MNTH_AVG_SPREAD",
    "USDRUB"."open" as usdrub_open,
    "USDRUB"."high"  as usdrub_high,
    "USDRUB"."low" as usdrub_low,
    "USDRUB"."close" as usdrub_close,
    "USDRUB"."vol" as usdrub_vol,
    "BRN"."open" as brn_open,
    "BRN"."high" as brn_high,
    "BRN"."low" as brn_low,
    "BRN"."close" as brn_close,
    "BRN"."vol" as brn_vol
FROM "RTSI"
         INNER JOIN "RTSI_HIST_DAY" ON
            "RTSI"."ticker" = "RTSI_HIST_DAY"."ticker" AND
            "RTSI"."per" = "RTSI_HIST_DAY"."per" AND
            date_part('day', "RTSI"."date") = "RTSI_HIST_DAY"."date_part"
         INNER JOIN "RTSI_HIST_MNTH" ON
            "RTSI"."ticker" = "RTSI_HIST_MNTH"."ticker" AND
            "RTSI"."per" = "RTSI_HIST_MNTH"."per" AND
            date_part('month', "RTSI"."date") = "RTSI_HIST_MNTH"."date_part"
         LEFT OUTER JOIN "prices_imported" AS "USDRUB" ON
            "USDRUB"."date" = "RTSI"."date" AND "USDRUB"."time" = "RTSI"."time" AND "USDRUB"."per" = "RTSI"."per" AND
            "USDRUB"."ticker" = 'USDRUB'
         LEFT OUTER JOIN "prices_imported" AS "BRN" ON
            "BRN"."date" = "RTSI"."date" AND "BRN"."time" = "RTSI"."time" AND "BRN"."per" = "RTSI"."per" AND
            "BRN"."ticker" = 'ICE.BRN'
ORDER BY "RTSI"."date", "RTSI"."time";


