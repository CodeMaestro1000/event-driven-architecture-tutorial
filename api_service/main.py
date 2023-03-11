from fastapi import FastAPI
from confluent_kafka import Producer
import json

from pydantic import BaseModel


class User(BaseModel):
    username: str
    email: str
    password: str


app = FastAPI()

producer = Producer({
    'bootstrap.servers': 'your-bootstrap-server-here',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'your-api-key-here',
    'sasl.password': 'your-api-secret-here',
    'sasl.mechanism': 'PLAIN',
})

@app.post("/login")
async def root(user: User):
    message = json.dumps({'message': f"Welcome {user.username}", "email": user.email})
    producer.produce('your-topic-name', message)
    producer.flush()    
    return {"message": f"Welcome {user.username}"}