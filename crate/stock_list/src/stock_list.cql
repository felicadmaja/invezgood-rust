-- Keyspace invezgood (SimpleStrategy, RF 1 — sesuaikan untuk production).
CREATE KEYSPACE IF NOT EXISTS invezgood
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE invezgood;

CREATE TABLE IF NOT EXISTS stock_list (
    code   text PRIMARY KEY,
    name   text,
    sector text,
    logo   text
);
