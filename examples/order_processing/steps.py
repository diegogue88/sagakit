from __future__ import annotations

import os
from typing import Any

from sagakit import SagaContext, step

_FAIL_AT = os.environ.get("FAIL_AT_STEP")


@step(compensate="refund_payment")
async def charge_payment(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
    """Charge the customer's payment method.

    Compensation: refund_payment.
    Set FAIL_AT_STEP=charge_payment to simulate a gateway timeout.
    """
    if _FAIL_AT == "charge_payment":
        raise RuntimeError("Payment gateway timeout")
    payment_id = f"pay_{ctx.saga_id[:8]}"
    ctx.logger.info("payment.charged", payment_id=payment_id, amount=ctx.saga_input["amount"])
    return {"payment_id": payment_id}


@step(compensate="release_inventory")
async def reserve_inventory(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
    """Reserve the requested items in the warehouse.

    Compensation: release_inventory.
    Set FAIL_AT_STEP=reserve_inventory to simulate an inventory service outage.
    """
    if _FAIL_AT == "reserve_inventory":
        raise RuntimeError("Inventory service unavailable")
    reservation_id = f"res_{ctx.saga_id[:8]}"
    ctx.logger.info(
        "inventory.reserved", reservation_id=reservation_id, items=ctx.saga_input["items"]
    )
    return {"reservation_id": reservation_id}


@step
async def ship_order(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
    """Create a shipping label (last step — no compensation required).

    Set FAIL_AT_STEP=ship_order to simulate a shipping API outage.
    """
    if _FAIL_AT == "ship_order":
        raise RuntimeError("Shipping API down")
    tracking_id = f"trk_{ctx.saga_id[:8]}"
    ctx.logger.info("order.shipped", tracking_id=tracking_id, order_id=ctx.saga_input["order_id"])
    return {"tracking_id": tracking_id}


@step
async def refund_payment(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
    """Refund the payment charged by charge_payment."""
    payment_id = f"pay_{ctx.saga_id[:8]}"
    ctx.logger.info("payment.refunded", payment_id=payment_id)
    return {"refunded_payment_id": payment_id}


@step
async def release_inventory(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
    """Release the inventory reservation made by reserve_inventory."""
    reservation_id = f"res_{ctx.saga_id[:8]}"
    ctx.logger.info("inventory.released", reservation_id=reservation_id)
    return {"released_reservation_id": reservation_id}
