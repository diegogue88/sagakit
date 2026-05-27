# Order Processing Example

Demonstrates sagakit's three-step saga with full compensation. When any step
fails, previously completed steps are rolled back in reverse order using their
registered compensation handlers.

## Steps

| Step | Compensation |
|---|---|
| `charge_payment` | `refund_payment` |
| `reserve_inventory` | `release_inventory` |
| `ship_order` | *(last step — no compensation)* |

## Prerequisites

- Python 3.11+
- Docker (to run Redis)
- `uv` with the sagakit environment synced: `uv sync`

## Running the example

```bash
# From the repo root — start Redis
docker run -d -p 6379:6379 redis:7-alpine redis-server --appendonly yes

# Happy path (from repo root)
uv run python -m examples.order_processing.run

# Simulate failure at ship_order (triggers compensation of charge_payment + reserve_inventory)
FAIL_AT_STEP=ship_order uv run python -m examples.order_processing.run

# Simulate failure at charge_payment (no steps completed, nothing to compensate)
FAIL_AT_STEP=charge_payment uv run python -m examples.order_processing.run
```

## Expected output — happy path

```
2026-05-27 10:00:00 [info     ] payment.charged    amount=99.99 payment_id=pay_a1b2c3d4 saga_id=... saga_name=order_processing step_name=charge_payment
2026-05-27 10:00:00 [info     ] inventory.reserved items=['item-a', 'item-b'] reservation_id=res_a1b2c3d4 saga_id=... saga_name=order_processing step_name=reserve_inventory
2026-05-27 10:00:00 [info     ] order.shipped      order_id=ord-001 saga_id=... saga_name=order_processing step_name=ship_order tracking_id=trk_a1b2c3d4

Status   : completed
Saga ID  : a1b2c3d4e5f6...
Results  :
  charge_payment: {'payment_id': 'pay_a1b2c3d4'}
  reserve_inventory: {'reservation_id': 'res_a1b2c3d4'}
  ship_order: {'tracking_id': 'trk_a1b2c3d4'}
```

Exit code: `0`

## Expected output — FAIL_AT_STEP=ship_order

`charge_payment` and `reserve_inventory` succeed, then `ship_order` fails after
3 retry attempts. sagakit compensates in reverse order:
`release_inventory` → `refund_payment`.

```
2026-05-27 10:00:00 [info     ] payment.charged    amount=99.99 payment_id=pay_a1b2c3d4 ...
2026-05-27 10:00:00 [info     ] inventory.reserved items=['item-a', 'item-b'] reservation_id=res_a1b2c3d4 ...
2026-05-27 10:00:00 [warning  ] step.retrying      attempt=1 delay=0.07 error=Shipping API down step=ship_order ...
2026-05-27 10:00:00 [warning  ] step.retrying      attempt=2 delay=0.13 error=Shipping API down step=ship_order ...
2026-05-27 10:00:00 [error    ] step.exhausted_retries attempts=3 error=Shipping API down step=ship_order ...
2026-05-27 10:00:00 [info     ] inventory.released reservation_id=res_a1b2c3d4 ...
2026-05-27 10:00:00 [info     ] payment.refunded   payment_id=pay_a1b2c3d4 ...

Status   : compensated
Saga ID  : a1b2c3d4e5f6...
Results  :
  charge_payment: {'payment_id': 'pay_a1b2c3d4'}
  reserve_inventory: {'reservation_id': 'res_a1b2c3d4'}
Failed at: ship_order
Error    : Shipping API down
Rolled back: reserve_inventory, charge_payment
```

Exit code: `1`
