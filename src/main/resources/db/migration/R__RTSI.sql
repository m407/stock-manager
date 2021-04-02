DROP VIEW IF EXISTS "RTSI";
CREATE VIEW "RTSI" AS
WITH "RTSI" AS (SELECT *,
                            AVG("close") OVER
                        (PARTITION BY "ticker", "per"
                        ORDER BY "date"
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS "SMA50",
                            AVG("close") OVER
                        (PARTITION BY "ticker", "per"
                        ORDER BY "date"
                        ROWS BETWEEN 200 PRECEDING AND CURRENT ROW) AS "SMA200"
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
    "USDRUB"."close" - "USDRUB"."open" AS "usdrub_close_open",
    "USDRUB"."vol" AS "usdrub_vol",
    "BRN"."close" - "BRN"."open" AS "brn_close_open",
    "BRN"."vol" AS "brn_vol",
    "SP500"."close" - "SP500"."open" AS "sp500_close_open",
    "SP500"."vol" AS "sp500__vol"
FROM "RTSI"
         INNER JOIN "RTSI_HIST_DAY" ON
        "RTSI"."ticker" = "RTSI_HIST_DAY"."ticker" AND
        "RTSI"."per" = "RTSI_HIST_DAY"."per" AND
        date_part('day', "RTSI"."date") = "RTSI_HIST_DAY"."date_part"
         INNER JOIN "RTSI_HIST_MNTH" ON
        "RTSI"."ticker" = "RTSI_HIST_MNTH"."ticker" AND
        "RTSI"."per" = "RTSI_HIST_MNTH"."per" AND
        date_part('month', "RTSI"."date") = "RTSI_HIST_MNTH"."date_part"
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
