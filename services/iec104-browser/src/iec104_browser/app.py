"""Read-only HTTP and WebSocket surface for currently received IEC 104 values."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles

from iec104_browser.hub import LiveHub


def create_app(hub: LiveHub, static_root: Path | None = None) -> FastAPI:
    """Create an API that retains IEC reception for its full lifespan."""

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await hub.start()
        try:
            yield
        finally:
            await hub.shutdown()

    app = FastAPI(
        title="WAMA IEC 104 Monitor",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/iec104/status")
    async def status() -> dict[str, object]:
        return await hub.status()

    @app.websocket("/v1/iec104/live")
    async def live(websocket: WebSocket) -> None:
        await websocket.accept()
        subscription = None
        try:
            subscription = await hub.subscribe()
            while True:
                queue_task = asyncio.create_task(subscription.queue.get())
                receive_task = asyncio.create_task(websocket.receive())
                done, pending = await asyncio.wait(
                    {queue_task, receive_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)

                if receive_task in done:
                    message = receive_task.result()
                    if message["type"] == "websocket.disconnect":
                        return
                if queue_task in done:
                    await websocket.send_json(queue_task.result())
        finally:
            if subscription is not None:
                await hub.unsubscribe(subscription)

    if static_root is not None:
        app.mount("/", StaticFiles(directory=static_root, html=True), name="static")

    return app