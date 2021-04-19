CREATE VIEW "c" AS
SELECT *
FROM "prices_imported"
WHERE "ticker" = 'RI.RTSI' AND "per" = '10';
