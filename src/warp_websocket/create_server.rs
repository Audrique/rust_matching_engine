use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use warp::Filter;
use crate::warp_websocket::{handler, handler::TopicActionRequest};
use crate::{Clients};


fn with_clients(clients: Clients) -> impl Filter<Extract = (Clients,), Error = Infallible> + Clone {
    warp::any().map(move || clients.clone())
}
pub async fn start_server(tx: oneshot::Sender<()>) {
    let clients: Clients = Arc::new(RwLock::new(HashMap::new()));
    let health_route = warp::path!("health").and_then(handler::health_handler);

    let register = warp::path("register");
    let register_routes = register
        .and(warp::post())
        .and(warp::body::json())
        .and(with_clients(clients.clone()))
        .and_then(handler::register_handler)
        .or(register
            .and(warp::delete())
            .and(warp::path::param())
            .and(with_clients(clients.clone()))
            .and_then(handler::unregister_handler)
        );

    let publish = warp::path!("publish")
        .and(warp::body::json())
        .and(with_clients(clients.clone()))
        .and_then(handler::publish_handler);

    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(warp::path::param())
        .and(with_clients(clients.clone()))
        .and_then(handler::ws_handler);

    let clients_for_add = clients.clone();
    let add_topic_route = warp::post()
        .and(warp::path("add_topic"))
        .and(warp::body::json::<TopicActionRequest>())
        .and(warp::any().map(move || clients_for_add.clone()))
        .and_then(handler::add_topic);

    let clients_for_remove = clients.clone();
    let remove_topic_route = warp::delete()
        .and(warp::path("remove_topic"))
        .and(warp::body::json::<TopicActionRequest>())
        .and(warp::any().map(move || clients_for_remove.clone()))
        .and_then(handler::remove_topic);

    let routes = health_route
        .or(register_routes)
        .or(ws_route)
        .or(publish)
        .or(add_topic_route)
        .or(remove_topic_route)
        .with(warp::cors().allow_any_origin());

    // TODO: make sure the correct cors are setup (needed for the react frontend)
    let cors = warp::cors()
        .allow_origin("http://localhost:5173")  // Allow any origin
        .allow_methods(vec!["GET", "POST", "DELETE", "OPTIONS"])
        .allow_headers(vec!["Content-Type", "Access-Control-Allow-Origin"])
        .allow_credentials(true);

    let routes_with_cors = routes.with(cors);

    // Signal that the server is about to start
    let _ = tx.send(());
    warp::serve(routes_with_cors).run(([127, 0, 0, 1], 8000)).await;
}