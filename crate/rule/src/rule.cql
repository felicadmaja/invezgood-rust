=== Struktur migrasi stockbit.rule (schema summary) ===

Keyspace: "stockbit"

--- Tabel dasar (base table) ---
Nama penuh: "stockbit"."rule"
Primary key: (("rule_name")) — text.
Kolom dan tipe CQL:
  "rule_name" text — partition key
  "rule_description" text
  "rule_parameter" text
  "rule_gt_or_lt" tinyint
  "rule_value" double

Gunakan: lookup rule per rule_name (contoh WHERE rule_name = ?).

=== Akhir ringkasan struktur ===
