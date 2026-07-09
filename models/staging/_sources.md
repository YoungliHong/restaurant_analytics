# Raw Source Contract — restaurant_pipeline

| Table         | Grain                          | Primary Key      | Notes                                    |
|---------------|--------------------------------|-------------------|-------------------------------------------|
| restaurants   | one row per location            | restaurant_id     | static dim, 4 rows                        |
| menu_items    | one row per menu item            | menu_item_id      | static dim, base_price is list price only |
| customers     | one row per loyalty customer     | customer_id       | guests (non-loyalty orders) not represented here |
| orders        | one row per order (header only)  | order_id          | NOT guaranteed unique — ~0.4% duplicated by design, dedup in Phase 3.1; customer_id nullable (guest orders) |
| order_items   | one row per line item on an order| order_item_id     | unit_price is snapshotted at order time — do NOT join to menu_items.base_price for revenue |

orders.restaurant_id → restaurants.restaurant_id, order_items.order_id → orders.order_id
