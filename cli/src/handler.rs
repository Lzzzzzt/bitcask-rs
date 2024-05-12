use std::sync::Arc;

use crate::request::{GetBody, PutBody};
use axum::{extract::State, Json};
use bitcask_rs_core::db::Engine;

use serde_json::{json, Value};

pub async fn bitcask_get(
    State(engine): State<Arc<Engine>>,
    Json(body): Json<GetBody>,
) -> Json<Value> {
    match engine.get(body.key) {
        Ok(value) => Json(json!({ "state": "success", "value": value })),
        Err(e) => Json(json!({ "state": e.to_string() })),
    }
}

pub async fn bitcask_put(
    State(engine): State<Arc<Engine>>,
    Json(body): Json<PutBody>,
) -> Json<Value> {
    match engine.put(body.key, body.value) {
        Ok(_) => Json(json!({ "state": "success" })),
        Err(e) => Json(json!({ "state": e.to_string() })),
    }
}
