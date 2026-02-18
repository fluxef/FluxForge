-- -----------------------------------------------------
-- 1. Tabelle 'bla': Fokus auf Numerik, Zeit und Keys
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS bla
(
    id_primary  INT               NOT NULL AUTO_INCREMENT,
    t_tiny      TINYINT           NULL,
    t_tiny_nn   TINYINT           NOT NULL,
    t_small     SMALLINT UNSIGNED NULL,
    t_medium    MEDIUMINT         NULL,
    t_big       BIGINT            NULL,
    t_decimal   DECIMAL(10, 2)    NULL,
    t_float     FLOAT             NULL,
    t_double    DOUBLE            NULL,
    t_date      DATE              NULL,
    t_datetime  DATETIME(3)       NULL,
    t_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    t_time      TIME              NULL,
    t_year      YEAR              NULL,
    t_bit       BIT(8)            NULL,
    PRIMARY KEY (id_primary),
    INDEX idx_decimal (t_decimal),
    UNIQUE INDEX uq_big (t_big),
    INDEX idx_multi_numeric (t_tiny_nn, t_small)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

-- -----------------------------------------------------
-- 2. Tabelle 'fasel': Fokus auf Strings, Blobs und JSON
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS fasel
(
    id_primary   INT                              NOT NULL AUTO_INCREMENT,
    t_char       CHAR(10)                         NULL,
    t_varchar    VARCHAR(255)                     NOT NULL,
    t_tinytext   TINYTEXT                         NULL,
    t_text       TEXT                             NULL,
    t_mediumtext MEDIUMTEXT                       NULL,
    t_longtext   LONGTEXT                         NULL,
    t_binary     BINARY(16)                       NULL,
    t_varbinary  VARBINARY(100)                   NULL,
    t_blob       BLOB                             NULL,
    t_enum       ENUM ('klein', 'mittel', 'groß') NULL,
    t_set        SET ('rot', 'grün', 'blau')      NULL,
    t_json       JSON                             NULL,
    PRIMARY KEY (id_primary),
    INDEX idx_varchar (t_varchar(20)),
    UNIQUE INDEX uq_char (t_char),
    FULLTEXT INDEX ft_text (t_text)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

-- -----------------------------------------------------
-- 3. Testdaten für 'bla'
-- -----------------------------------------------------
INSERT INTO bla (t_tiny, t_tiny_nn, t_small, t_medium, t_big, t_decimal, t_float, t_double, t_date, t_datetime, t_time,
                 t_year, t_bit)
VALUES (1, 10, 100, 1000, 1000000, 12.34, 1.23, 3.1415, '2023-01-01', '2023-01-01 12:00:00', '12:00:00', 2023,
        b'00000001'),
       (-1, 20, 0, -100, -5000, 99.99, 0.0, 0.0, '1970-01-01', '1999-12-31 23:59:59.999', '00:00:00', 1901,
        b'11111111'),
       (NULL, 30, 65535, 8388607, 9223372036854775807, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
       (127, 40, NULL, NULL, -9223372036854775808, -12.50, -0.5, 1.234567, '9999-12-31', '2038-01-19 03:14:07',
        '23:59:59', 2155, b'10101010'),
       (0, 50, 1, 1, 1, 0.00, 1.1, 2.2, '2024-02-29', '2024-02-29 08:30:00', '08:30:00', 2024, b'00000000'),
       (10, 60, 5, 5, 500, 100.00, 100.0, 100.0, '2000-01-01', '2000-01-01 00:00:00', '01:01:01', 2000, b'01010101'),
       (NULL, 70, 2, 2, 2, NULL, 3.3, 4.4, NULL, '2025-12-24 18:00:00', '18:00:00', 2025, NULL),
       (-128, 80, 10, 10, 10, 0.01, 0.001, 0.00001, '1000-01-01', '1000-01-01 00:00:00', '12:00:00', 1945, b'00001111'),
       (1, 90, 1000, 2000, 3000, 5.55, 5.5, 5.5, '2021-05-05', '2021-05-05 05:05:05', '05:05:05', 2021, b'11110000'),
       (5, 100, NULL, NULL, 99999, 9.99, 9.9, 9.9, '2022-06-06', NULL, NULL, 2022, b'00110011');

-- -----------------------------------------------------
-- 4. Testdaten für 'fasel'
-- -----------------------------------------------------
INSERT INTO fasel (t_char, t_varchar, t_tinytext, t_text, t_mediumtext, t_longtext, t_binary, t_varbinary, t_blob,
                   t_enum, t_set, t_json)
VALUES ('ABC', 'Standard Text', 'Tiny', 'Das ist ein normaler Text.', 'Medium Content', 'Long Content...',
        0x12345600000000000000000000000000, 0xABCDEF, 0xDEADBEEF, 'mittel', 'rot,grün', '{
    "key": "value",
    "id": 1
  }'),
       (NULL, 'Anderer Text', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
       ('UNIQUE1', 'Pflichtfeld', 'Test', 'Suche nach Fulltext', 'More data', 'Even more',
        0x00000000000000000000000000000000, 0x112233, 0x445566, 'klein', 'blau', '{
         "active": true,
         "tags": [
           1,
           2,
           3
         ]
       }'),
       ('MAX', REPEAT('X', 255), 'T', 'Text', 'M', 'L', 0x01000000000000000000000000000000, 0x02, 0x03, 'groß',
        'rot,blau', '[
         1,
         2,
         3
       ]'),
       ('MIN', '', '', '', '', '', 0x00000000000000000000000000000000, 0x00, 0x00, 'mittel', '', '{}'),
       (NULL, 'Emojis: 🚀🦀', '🦀', 'Rust is cool', '...', '...', NULL, 0x12, 0x34, 'klein', 'grün', '{
         "lang": "Rust"
       }'),
       ('123', 'Numbers', '123', '456', '789', '000', 0x31323300000000000000000000000000, 0x343536, 0x373839, 'groß',
        'rot,grün,blau', '{
         "pi": 3.14
       }'),
       ('EMPTY', 'Empty fields', NULL, '', NULL, '', NULL, NULL, NULL, NULL, 'rot', 'null'),
       ('JSON_TEST', 'JSON Object', 'json', 'content', 'medium', 'long', 0x01000000000000000000000000000000, 0x02, 0x03,
        'mittel', 'grün,blau', '{
         "nested": {
           "a": 1
         }
       }'),
       ('LAST', 'Last entry', 'Ende', 'Over', 'Finish', 'Done', 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, 0xFF, 0xFF, 'groß',
        'blau', '{
         "status": "ok"
       }');

