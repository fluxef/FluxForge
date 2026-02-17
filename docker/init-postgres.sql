-- Tabelle 'foo': Numerik, UUID und Zeit
CREATE TABLE IF NOT EXISTS foo
(
    id_primary  SERIAL PRIMARY KEY,
    t_uuid UUID DEFAULT gen_random_uuid(),
    t_int       INTEGER          NOT NULL,
    t_bigint    BIGINT           NULL,
    t_numeric   NUMERIC(15, 2)   NULL,
    t_double    DOUBLE PRECISION NULL,
    t_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    t_date      DATE             NULL,
    t_boolean   BOOLEAN DEFAULT TRUE,
    t_inet INET NULL -- Spezieller PG Typ für IP-Adressen
);

-- Tabelle 'bar': Text, Arrays und JSONB
CREATE TABLE IF NOT EXISTS bar
(
    id_primary   SERIAL PRIMARY KEY,
    t_varchar    VARCHAR(255) NOT NULL,
    t_text       TEXT         NULL,
    t_int_array  INTEGER[] NULL, -- PG Array Typ
    t_text_array TEXT[] NULL,
    t_jsonb JSONB NULL,          -- Binäres JSON
    t_bytea BYTEA NULL           -- Binärdaten
);

-- Testdaten für 'foo'
INSERT INTO foo (t_int, t_bigint, t_numeric, t_double, t_date, t_boolean, t_inet)
VALUES (100, 1000000000, 123.45, 3.1415, '2023-01-01', TRUE, '192.168.1.1'),
       (200, NULL, NULL, NULL, NULL, FALSE, NULL),
       (-1, -500, 0.01, 0.0001, '1999-12-31', TRUE, '2001:db8::1');

-- Testdaten für 'bar'
INSERT INTO bar (t_varchar, t_text, t_int_array, t_text_array, t_jsonb, t_bytea)
VALUES ('Test 1', 'Langer Text', '{1,2,3}', '{"A","B"}', '{"status": "ok", "tags": [1,2]}', '\xDEADBEEF'),
       ('Leer', NULL, NULL, NULL, '{}', NULL);


