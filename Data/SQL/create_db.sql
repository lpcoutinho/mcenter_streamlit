CREATE TABLE "tiny_ml_codes" (
  "ml_inventory_id" varchar(30) PRIMARY KEY,
  "ml_code" varchar(30),
  "ml_sku" varchar(150),
  "created_at" timestamp DEFAULT (now() at time zone 'utc'),
  "updated_at" timestamp
);

CREATE TABLE "tiny_codes" (
  "ml_inventory_id" varchar(30) PRIMARY KEY,
  "tiny_id" int,
  "tiny_sku" varchar(150),
  "created_at" timestamp DEFAULT (now() at time zone 'utc'),
  "updated_at" timestamp
);

CREATE TABLE "tiny_products" (
  "tiny_id" int PRIMARY KEY,
  "data_criacao" varchar(19),
  "nome" varchar(150),
  "sku_tiny" varchar(30),
  "unidade" varchar(10),
  "preco" decimal,
  "ncm" varchar(10),
  "origem" varchar(1),
  "gtin" varchar(14),
  "peso_bruto" decimal DEFAULT null,
  "estoque_minimo" decimal DEFAULT null,
  "estoque_maximo" decimal DEFAULT null,
  "id_fornecedor" int,
  "nome_fornecedor" varchar(150),
  "codigo_pelo_fornecedor" varchar(20),
  "preco_custo" decimal,
  "preco_custo_medio" decimal,
  "situacao" varchar(1),
  "tipo" varchar(1),
  "cest" varchar(9),
  "marca" varchar(150),
  "tipo_embalagem" int,
  "altura_embalagem" decimal,
  "comprimento_embalagem" decimal,
  "largura_embalagem" decimal,
  "diametro_embalagem" decimal,
  "qtd_volumes" decimal,
  "categoria" varchar(255),
  "created_at" timestamp DEFAULT (now() at time zone 'utc'),
  "updated_at" timestamp
);

CREATE TABLE "tiny_fulfillment" (
  "ml_inventory_id" varchar(30) PRIMARY KEY,
  "ml_code" varchar(30),
  "ml_sku" varchar(150),
  "var_code" varchar(30),
  "tiny_id" varchar(30),
  "tiny_sku" varchar(150),
  "ad_title" varchar(150),
  "created_at" timestamp DEFAULT (now() at time zone 'utc'),
  "updated_at" timestamp
);

CREATE TABLE "tiny_stock_hist" (
  "tiny_id" int,
  "nome" varchar(150),
  "sku_tiny" varchar(30),
  "unidade" varchar(10),
  "saldo_reservado" decimal,
  "deposito_nome" varchar(150),
  "deposito_desconsiderar" varchar(1),
  "deposito_saldo" decimal,
  "deposito_empresa" varchar(150),
  "created_at" timestamp DEFAULT (now() at time zone 'utc')
);

CREATE TABLE "items" (
  "ml_code" varchar(30),
  "inventory_id" varchar(30),
  "value_name" varchar(30),
  "status" varchar(30),
  "catalog_listing" bool,
  "created_at" timestamp DEFAULT (now() at time zone 'utc'),
  "updated_at" timestamp DEFAULT (now() at time zone 'utc')
);


ALTER TABLE "tiny_codes" ADD FOREIGN KEY ("ml_inventory_id") REFERENCES "tiny_ml_codes" ("ml_inventory_id");
