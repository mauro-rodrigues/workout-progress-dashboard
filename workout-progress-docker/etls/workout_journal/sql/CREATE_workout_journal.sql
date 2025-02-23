CREATE TABLE IF NOT EXISTS {schema}.{table_name}
(
    month text COLLATE pg_catalog."default" NOT NULL,
    weekday text COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    deload smallint NOT NULL,
    workout_counter smallint NOT NULL,
    set_counter smallint NOT NULL,
    type text COLLATE pg_catalog."default" NOT NULL,
    section text COLLATE pg_catalog."default" NOT NULL,
    exercise text COLLATE pg_catalog."default" NOT NULL,
    weight numeric(5,2) NOT NULL,
    weight_unit text COLLATE pg_catalog."default" NOT NULL,
    grip text COLLATE pg_catalog."default" NOT NULL,
    amount smallint NOT NULL,
    rest smallint NOT NULL,
    CONSTRAINT workout_journal_weight_unit_check CHECK (weight_unit = ANY (ARRAY['kg'::text, 'lbs'::text]))
);