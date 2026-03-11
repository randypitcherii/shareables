from fastapi import FastAPI

from server.routes import router

app = FastAPI(
    title="Scoped OAuth LLM Wrapper",
    description="User-scoped policy enforcement and auditable LLM/tool API wrapper.",
    version="0.1.0",
)

app.include_router(router)
