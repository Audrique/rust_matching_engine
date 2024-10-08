use std::collections::HashMap;
use std::sync::Arc;
use crate::{Client, Clients, Result};
use crate::warp_websocket::ws;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;
use warp::{http::StatusCode, reply::json, ws::Message, Reply};
use crate::matching_engine::engine::MatchingEngine;
use tokio::sync::{Mutex as TokioMutex, RwLock};
use crate::connecting_to_exchanges::for_all_exchanges::TraderData;

#[derive(Deserialize, Debug)]
pub struct RegisterRequest {
    user_id: usize,
    topic: String,
}

#[derive(Deserialize)]
pub struct TopicActionRequest {
    topic: String,
    client_id: String,
}


#[derive(Serialize, Debug)]
pub struct RegisterResponse {
    url: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Event {
    topic: String,
    user_id: Option<usize>,
    message: String,
}

impl Event {
    pub fn new(topic: String, user_id: Option<usize>, message: String) -> Event {
        Event {
            topic,
            user_id,
            message
        }
    }
}

pub async fn publish_handler(body: Event,
                             clients: Clients,
                             topic_counters: Arc<RwLock<HashMap<String, u32>>>) -> Result<impl Reply> {
    let mut counters = topic_counters.write().await;
    let counter = counters.entry(body.topic.clone()).or_insert(0);
    clients
        .read()
        .await
        .iter()
        .filter(|(_, client)| match body.user_id {
            Some(v) => client.user_id == v,
            None => true,
        })
        .filter(|(_, client)| client.topics.contains(&body.topic))
        .for_each(|(_, client)| {
            if let Some(sender) = &client.sender {
                let json_message = json!({ "topic": body.topic,
                    "user_id": body.user_id,
                    "update_counter": *counter,
                    "message": body.message });
                let _ = sender.send(Ok(Message::text(json_message.to_string())));

            }
        });
    *counter += 1;
    Ok(StatusCode::OK)
}

pub async fn register_handler(body: RegisterRequest, clients: Clients) -> Result<impl Reply> {
    let user_id = body.user_id;
    let topic = body.topic; // Capture the entry topic
    let uuid = Uuid::new_v4().as_simple().to_string();

    register_client(uuid.clone(), user_id, topic, clients).await; // Pass the entry topic
    Ok(json(&RegisterResponse {
        url: format!("ws://127.0.0.1:8000/ws/{}", uuid),
    }))
}

async fn register_client(id: String, user_id: usize, topic: String, clients: Clients) {
    clients.write().await.insert(
        id,
        Client {
            user_id,
            topics: vec![topic],
            sender: None,
        },
    );
}

pub async fn unregister_handler(id: String, clients: Clients) -> Result<impl Reply> {
    clients.write().await.remove(&id);
    Ok(StatusCode::OK)
}

pub async fn ws_handler(ws: warp::ws::Ws,
                        id: String,
                        clients: Clients,
                        matching_engine: Arc<TokioMutex<MatchingEngine>>,
                        traders_data: Arc<TokioMutex<HashMap<String, TraderData>>>
                        ) -> Result<impl Reply> {
    let client = clients.read().await.get(&id).cloned();
    match client {
        Some(c) => Ok(ws.on_upgrade(move |socket| ws::client_connection(socket, id, clients, c, matching_engine, traders_data))),
        None => Err(warp::reject::not_found()),
    }
}

pub async fn health_handler() -> Result<impl Reply> {
    Ok(StatusCode::OK)
}



pub async fn add_topic(body: TopicActionRequest, clients: Clients) -> Result<impl Reply> {
    let mut clients_write = clients.write().await;
    if let Some(client) = clients_write.get_mut(&body.client_id) {
        client.topics.push(body.topic);
    }
    Ok(warp::reply::with_status("Added topic successfully", StatusCode::OK))
}

pub async fn remove_topic(body: TopicActionRequest, clients: Clients) -> Result<impl Reply> {
    let mut clients_write = clients.write().await;
    if let Some(client) = clients_write.get_mut(&body.client_id) {
        client.topics.retain(|t| t != &body.topic);
    }
    Ok(warp::reply::with_status("Removed topic successfully", StatusCode::OK))
}