import os, uvicorn, asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager

app = FastAPI()
bot_task = None

@asynccontextmanager
async def lifespan(app):
    global bot_task
    from bot import main
    bot_task = asyncio.create_task(main())
    print("✅ Bot started")
    yield
    if bot_task:
        bot_task.cancel()
        await bot_task

app.router.lifespan_context = lifespan

@app.get("/")
async def health():
    return {"status": "running"}

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv('PORT', 10000)))
