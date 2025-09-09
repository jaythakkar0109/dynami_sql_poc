from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.logger import init_logger
from app.routes import router
from app.settings import settings

app = FastAPI(
    title=settings.TITLE,
    description=settings.DESCRIPTION,
    version=settings.VERSION
)
# Initialize logger
init_logger(app)
# Configure middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)