from fastapi import FastAPI
import logging as log

logger = log.getLogger("uvicorn.error")
log.basicConfig(
    level=log.INFO,  
    format='%(asctime)s [%(levelname)s] %(message)s'
)

class EventStore:

    def __init__(self, max_events_per_user=10):
        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        """
        Сохраняет событие
        """
        log.info(f'in put: {self.events}')
        user_events = self.events.get(user_id, [])  
        self.events[user_id] = [item_id] + user_events[: self.max_events_per_user]
        log.info(f"in put: {self.events}")

    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        log.info(self.events)
        user_events = self.events.get(user_id, [])  # если пользователя нет — пустой список
        return user_events[:k]  


# создаём глобальный стор

events_store = EventStore()

# создаём приложение FastAPI

app = FastAPI(title="events")

@app.post("/put")
async def put(user_id: int, item_id: int):
    """
    Сохраняет событие для user_id, item_id
    """
    log.info(events_store)
    events_store.put(user_id, item_id)
    log.info(events_store)
    return {"result": "ok"}

@app.get("/get")
async def get(user_id: int, k: int = 10):
    """
    Возвращает список последних k событий для пользователя user_id
    """
    events = events_store.get(user_id, k)
    log.info(events)
    log.info(type(events))
    return {"events": events}

