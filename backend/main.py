from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

app = FastAPI()

STATIC_DIR = Path(__file__).parent / "static"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/{full_path:path}")
def serve_dashboard(full_path: str):
    return FileResponse(STATIC_DIR / "index.html")
