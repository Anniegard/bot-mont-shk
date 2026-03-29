from __future__ import annotations

from dataclasses import asdict, dataclass

from fastapi import Request

from bot.services.workflows import (
    EXPECTED_24H,
    EXPECTED_NO_MOVE,
    EXPECTED_WAREHOUSE_DELAY_MULTIPLE,
    EXPECTED_WAREHOUSE_DELAY_SINGLE,
)

SESSION_FLOW_KEY = "web_flow"

WEB_MODE_NO_MOVE = EXPECTED_NO_MOVE
WEB_MODE_24H = EXPECTED_24H
WEB_MODE_WAREHOUSE_DELAY = "warehouse_delay"


@dataclass(frozen=True)
class FlowState:
    mode: str | None = None
    no_move_export_mode: str | None = None
    warehouse_delay_mode: str | None = None

    @property
    def active_processing_mode(self) -> str | None:
        if self.mode == WEB_MODE_NO_MOVE and self.no_move_export_mode:
            return EXPECTED_NO_MOVE
        if self.mode == WEB_MODE_24H:
            return EXPECTED_24H
        if (
            self.mode == WEB_MODE_WAREHOUSE_DELAY
            and self.warehouse_delay_mode == EXPECTED_WAREHOUSE_DELAY_SINGLE
        ):
            return EXPECTED_WAREHOUSE_DELAY_SINGLE
        if (
            self.mode == WEB_MODE_WAREHOUSE_DELAY
            and self.warehouse_delay_mode == EXPECTED_WAREHOUSE_DELAY_MULTIPLE
        ):
            return EXPECTED_WAREHOUSE_DELAY_MULTIPLE
        return None

    @property
    def can_accept_file_or_url(self) -> bool:
        return self.active_processing_mode in {
            EXPECTED_NO_MOVE,
            EXPECTED_24H,
            EXPECTED_WAREHOUSE_DELAY_SINGLE,
        }


def get_flow_state(request: Request) -> FlowState:
    raw_state = request.session.get(SESSION_FLOW_KEY) or {}
    return FlowState(
        mode=raw_state.get("mode"),
        no_move_export_mode=raw_state.get("no_move_export_mode"),
        warehouse_delay_mode=raw_state.get("warehouse_delay_mode"),
    )


def save_flow_state(request: Request, state: FlowState) -> FlowState:
    request.session[SESSION_FLOW_KEY] = asdict(state)
    return state


def set_mode(request: Request, mode: str) -> FlowState:
    if mode == WEB_MODE_NO_MOVE:
        return save_flow_state(
            request,
            FlowState(
                mode=WEB_MODE_NO_MOVE,
                no_move_export_mode=None,
                warehouse_delay_mode=None,
            ),
        )
    if mode == WEB_MODE_24H:
        return save_flow_state(
            request,
            FlowState(
                mode=WEB_MODE_24H,
                no_move_export_mode=None,
                warehouse_delay_mode=None,
            ),
        )
    return save_flow_state(
        request,
        FlowState(
            mode=WEB_MODE_WAREHOUSE_DELAY,
            no_move_export_mode=None,
            warehouse_delay_mode=None,
        ),
    )


def set_no_move_export_mode(request: Request, export_mode: str) -> FlowState:
    state = get_flow_state(request)
    return save_flow_state(
        request,
        FlowState(
            mode=WEB_MODE_NO_MOVE,
            no_move_export_mode=export_mode,
            warehouse_delay_mode=state.warehouse_delay_mode,
        ),
    )


def set_warehouse_delay_mode(request: Request, mode: str) -> FlowState:
    state = get_flow_state(request)
    return save_flow_state(
        request,
        FlowState(
            mode=WEB_MODE_WAREHOUSE_DELAY,
            no_move_export_mode=state.no_move_export_mode,
            warehouse_delay_mode=mode,
        ),
    )
