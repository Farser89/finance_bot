create schema my_finance;

CREATE TABLE my_finance.purchases (
	id serial primary key, 
    date DATE ,
    category TEXT,
    details TEXT,
    amount INTEGER,
    card_type TEXT,
    bank text,
    is_accounted INT
);

CREATE TABLE my_finance.category (
	id serial primary key, 
    category TEXT
);

CREATE TABLE my_finance.card_deposits (
	id serial primary key,
    date DATE,
    card_deposit integer,
    bank VARCHAR(100),
    card_type VARCHAR(50),
    is_accounted INT
);

CREATE TABLE my_finance.bank_account (
	id serial primary key,
    bank VARCHAR(100),
    balance INT,
    card_type VARCHAR(50),
    credit_limit INT,
    debt INT
);