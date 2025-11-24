ALTER TABLE `giga_raw.raw_pos_transactions`
SET OPTIONS (
  description = "Raw POS transactions - DQ-enabled, optimized for big data"
);

ALTER TABLE `giga_raw.raw_ecom_orders`
SET OPTIONS (
  description = "Raw e-commerce orders - DQ-enabled, optimized for big data"
);

ALTER TABLE `giga_raw.raw_crm_customers`
SET OPTIONS (
  description = "Raw CRM customers - DQ-enabled, optimized for big data"
);

ALTER TABLE `giga_raw.raw_inventory_snapshots`
SET OPTIONS (
  description = "Raw inventory snapshots - DQ-enabled, optimized for big data"
);

-- Set sensible partition retention to manage storage costs
ALTER TABLE `giga_raw.raw_pos_transactions`
SET OPTIONS (
  partition_expiration_days = 365 -- Keep POS data for 1 year
);

ALTER TABLE `giga_raw.raw_ecom_orders`
SET OPTIONS (
  partition_expiration_days = 730 -- Keep e-commerce data for 2 years
);

ALTER TABLE `giga_raw.raw_crm_customers`
SET OPTIONS (
  partition_expiration_days = 1095 -- Keep CRM data for 3 years
);

ALTER TABLE `giga_raw.raw_inventory_snapshots`
SET OPTIONS (
  partition_expiration_days = 365 -- Keep inventory snapshots for 1 year
);
