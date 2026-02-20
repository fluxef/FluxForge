SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

CREATE TABLE IF NOT EXISTS bla
(
    id_primary  int       NOT NULL AUTO_INCREMENT,
    t_tiny      tinyint           DEFAULT NULL,
    t_tiny_nn   tinyint   NOT NULL,
    t_small     smallint UNSIGNED DEFAULT NULL,
    t_medium    mediumint         DEFAULT NULL,
    t_big       bigint            DEFAULT NULL,
    t_decimal   decimal(10, 2)    DEFAULT NULL,
    t_float     float             DEFAULT NULL,
    t_double    double            DEFAULT NULL,
    t_date      date              DEFAULT NULL,
    t_datetime  datetime(3)       DEFAULT NULL,
    t_timestamp timestamp NULL    DEFAULT CURRENT_TIMESTAMP,
    t_time      time              DEFAULT NULL,
    t_year      year              DEFAULT NULL,
    t_bit       bit(8)            DEFAULT NULL,
    PRIMARY KEY (id_primary),
    UNIQUE KEY uq_big (t_big),
    KEY idx_decimal (t_decimal),
    KEY idx_multi_numeric (t_tiny_nn, t_small)
) ENGINE = InnoDB
  AUTO_INCREMENT = 11
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

INSERT INTO bla (id_primary, t_tiny, t_tiny_nn, t_small, t_medium, t_big, t_decimal, t_float, t_double, t_date,
                 t_datetime, t_timestamp, t_time, t_year, t_bit)
VALUES (1, 1, 10, 100, 1000, 1000000, 12.34, 1.23, 3.1415, '2023-01-01', '2023-01-01 12:00:00.000',
        '2026-02-20 10:16:11', '12:00:00', '2023', b'00000001'),
       (2, -1, 20, 0, -100, -5000, 99.99, 0, 0, '1970-01-01', '1999-12-31 23:59:59.999', '2026-02-20 10:16:11',
        '00:00:00', '1901', b'11111111'),
       (3, NULL, 30, 65535, 8388607, 9223372036854775807, NULL, NULL, NULL, NULL, NULL, '2026-02-20 10:16:11', NULL,
        NULL, NULL),
       (4, 127, 40, NULL, NULL, -9223372036854775808, -12.50, -0.5, 1.234567, '9999-12-31', '2038-01-19 03:14:07.000',
        '2026-02-20 10:16:11', '23:59:59', '2155', b'10101010'),
       (5, 0, 50, 1, 1, 1, 0.00, 1.1, 2.2, '2024-02-29', '2024-02-29 08:30:00.000', '2026-02-20 10:16:11', '08:30:00',
        '2024', b'00000000'),
       (6, 10, 60, 5, 5, 500, 100.00, 100, 100, '2000-01-01', '2000-01-01 00:00:00.000', '2026-02-20 10:16:11',
        '01:01:01', '2000', b'01010101'),
       (7, NULL, 70, 2, 2, 2, NULL, 3.3, 4.4, NULL, '2025-12-24 18:00:00.000', '2026-02-20 10:16:11', '18:00:00',
        '2025', NULL),
       (8, -128, 80, 10, 10, 10, 0.01, 0.001, 0.00001, '1000-01-01', '1000-01-01 00:00:00.000', '2026-02-20 10:16:11',
        '12:00:00', '1945', b'00001111'),
       (9, 1, 90, 1000, 2000, 3000, 5.55, 5.5, 5.5, '2021-05-05', '2021-05-05 05:05:05.000', '2026-02-20 10:16:11',
        '05:05:05', '2021', b'11110000'),
       (10, 5, 100, NULL, NULL, 99999, 9.99, 9.9, 9.9, '2022-06-06', NULL, '2026-02-20 10:16:11', NULL, '2022',
        b'00110011');

CREATE TABLE IF NOT EXISTS fasel
(
    id_primary   int          NOT NULL AUTO_INCREMENT,
    t_char       char(10)                       DEFAULT NULL,
    t_varchar    varchar(255) NOT NULL,
    t_tinytext   tinytext,
    t_text       text,
    t_mediumtext mediumtext,
    t_longtext   longtext,
    t_binary     binary(16)                     DEFAULT NULL,
    t_varbinary  varbinary(100)                 DEFAULT NULL,
    t_blob       blob,
    t_enum       enum ('klein','mittel','groß') DEFAULT NULL,
    t_set        set ('rot','grün','blau')      DEFAULT NULL,
    t_json       json                           DEFAULT NULL,
    PRIMARY KEY (id_primary),
    UNIQUE KEY uq_char (t_char),
    KEY idx_varchar (t_varchar(20))
) ENGINE = InnoDB
  AUTO_INCREMENT = 11
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

INSERT INTO fasel (id_primary, t_char, t_varchar, t_tinytext, t_text, t_mediumtext, t_longtext, t_binary, t_varbinary,
                   t_blob, t_enum, t_set, t_json)
VALUES (1, 'ABC', 'Standard Text', 'Tiny', 'Das ist ein normaler Text.', 'Medium Content', 'Long Content...',
        0x12345600000000000000000000000000, 0xabcdef, 0xdeadbeef, 'mittel', 'rot,grün', '{
    \"id\": 1,
    \"key\": \"value\"
  }'),
       (2, NULL, 'Anderer Text', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
       (3, 'UNIQUE1', 'Pflichtfeld', 'Test', 'Suche nach Fulltext', 'More data', 'Even more',
        0x00000000000000000000000000000000, 0x112233, 0x445566, 'klein', 'blau', '{
         \"tags\": [
           1,
           2,
           3
         ],
         \"active\": true
       }'),
       (4, 'MAX',
        'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
        'T', 'Text', 'M', 'L', 0x01000000000000000000000000000000, 0x02, 0x03, 'groß', 'rot,blau', '[
         1,
         2,
         3
       ]'),
       (5, 'MIN', '', '', '', '', '', 0x00000000000000000000000000000000, 0x00, 0x00, 'mittel', '', '{}'),
       (6, NULL, 'Emojis: 🚀🦀', '🦀', 'Rust is cool', '...', '...', NULL, 0x12, 0x34, 'klein', 'grün', '{
         \"lang\": \"Rust\"
       }'),
       (7, '123', 'Numbers', '123', '456', '789', '000', 0x31323300000000000000000000000000, 0x343536, 0x373839, 'groß',
        'rot,grün,blau', '{
         \"pi\": 3.14
       }'),
       (8, 'EMPTY', 'Empty fields', NULL, '', NULL, '', NULL, NULL, NULL, NULL, 'rot', 'null'),
       (9, 'JSON_TEST', 'JSON Object', 'json', 'content', 'medium', 'long', 0x01000000000000000000000000000000, 0x02,
        0x03, 'mittel', 'grün,blau', '{
         \"nested\": {
           \"a\": 1
         }
       }'),
       (10, 'LAST', 'Last entry', 'Ende', 'Over', 'Finish', 'Done', 0xffffffffffffffffffffffffffffffff, 0xff, 0xff,
        'groß', 'blau', '{
         \"status\": \"ok\"
       }');

CREATE TABLE IF NOT EXISTS timey
(
    t_id                             int       NOT NULL AUTO_INCREMENT,
    t_timestamp_not_null             timestamp NOT NULL,
    t_timestamp_null                 timestamp NULL     DEFAULT NULL,
    t_timestamp_not_null_zerodefault timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
    t_datetime_not_null              datetime  NOT NULL,
    t_datetime_null                  datetime           DEFAULT NULL,
    t_datetime_not_null_zerodefault  datetime  NOT NULL DEFAULT '0000-00-00 00:00:00',
    t_date_null                      date               DEFAULT NULL,
    t_date_not_null                  date      NOT NULL,
    t_date_not_null_zerodefault      date      NOT NULL DEFAULT '0000-00-00',
    t_time_null                      time               DEFAULT NULL,
    t_time_not_null                  time      NOT NULL,
    t_time_not_null_zerodefault      time      NOT NULL DEFAULT '00:00:00',
    PRIMARY KEY (t_id)
) ENGINE = InnoDB
  AUTO_INCREMENT = 4
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

INSERT INTO timey (t_id, t_timestamp_not_null, t_timestamp_null, t_timestamp_not_null_zerodefault, t_datetime_not_null,
                   t_datetime_null, t_datetime_not_null_zerodefault, t_date_null, t_date_not_null,
                   t_date_not_null_zerodefault, t_time_null, t_time_not_null, t_time_not_null_zerodefault)
VALUES (1, '2026-02-20 12:48:54', '2026-02-20 11:22:27', '2026-02-20 12:22:27', '2026-02-20 13:22:27',
        '2026-02-20 14:22:27', '2026-02-20 15:22:27', '2026-02-20', '2026-02-20', '2026-02-20', '11:22:27', '12:22:27',
        '13:22:27'),
       (2, '2026-02-20 12:28:12', NULL, '0000-00-00 00:00:00', '0000-00-00 00:00:00', NULL, '0000-00-00 00:00:00', NULL,
        '0000-00-00', '0000-00-00', NULL, '00:00:00', '00:00:00');


ALTER TABLE fasel
    ADD FULLTEXT KEY ft_text (t_text);
