import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

# Load preferred environment file early so route/config imports see these vars.
runtime_env = os.getenv("APP_ENV", "dev").lower()
env_file = Path(__file__).parent / f"{runtime_env}.env"
if env_file.exists():
    load_dotenv(env_file)

from server.routes import api_router

app = FastAPI(
    title="UC Metadata Command Space",
    description="Review metadata quality and stage AI suggestions for Unity Catalog objects.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

frontend_dist = Path(__file__).parent / "frontend" / "dist"
assets_dir = frontend_dist / "assets"

if assets_dir.exists():
    app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")


@app.get("/", response_model=None)
def root():
    index_file = frontend_dist / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"status": "ok", "message": "Frontend build not found. Run npm run build in frontend/."}


@app.get("/{full_path:path}", response_model=None)
def spa_fallback(full_path: str):
    if full_path.startswith("api/"):
        return {"status": "not_found"}
    index_file = frontend_dist / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"status": "ok", "message": "Frontend build not found. Run npm run build in frontend/."}
