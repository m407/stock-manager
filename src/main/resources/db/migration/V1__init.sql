create table prices_imported
(
    ticker text not null,
    per text not null,
    date date not null,
    time time not null,
    open double precision not null,
    high double precision not null,
    low double precision not null,
    close double precision not null,
    vol bigint not null,
    constraint prices_imported_ticker_per_date_time_pk
        primary key (ticker, per, date, time)
);
