CREATE SCHEMA IF NOT EXISTS `giga_raw`
OPTIONS (
  location    = "asia-south1",
  description = "Raw landing zone (bronze) for GigaMart multi-source ingestion"
);

CREATE SCHEMA IF NOT EXISTS `giga_curated`
OPTIONS (
  location    = "asia-south1",
  description = "Curated / dimensional layer (silver/gold) for GigaMart analytics"
);

CREATE SCHEMA IF NOT EXISTS `giga_mart`
OPTIONS (
  location    = "asia-south1",
  description = "Business-facing marts, dashboards, and aggregate tables"
);
