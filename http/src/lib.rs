use std::{collections::HashMap, sync::Arc};

use actix_web::{delete, get, post, rt::spawn, web, HttpResponse, Responder};
use bitcask_rs_core::{db::Engine, errors::Errors};

#[post("/put")]
async fn put_handler(
    eng: web::Data<Arc<Engine>>,
    data: web::Json<HashMap<String, String>>,
) -> impl Responder {
    for (key, value) in data.0.into_iter() {
        if let Err(e) = eng.put(key, value) {
            return HttpResponse::InternalServerError()
                .body(format!("Failed to put value in engine, due to:\n{}", e));
        }
    }

    HttpResponse::Ok().body("OK")
}

#[get("/get/{key}")]
async fn get_handler(eng: web::Data<Arc<Engine>>, key: web::Path<String>) -> impl Responder {
    let value = match eng.get(key.as_bytes()) {
        Ok(val) => val,
        Err(e) => {
            if !matches!(e, Errors::KeyNotFound) {
                return HttpResponse::InternalServerError()
                    .body(format!("Failed to get value in engine, due to:\n{}", e));
            }
            return HttpResponse::Ok().body("Key Not Found");
        }
    };
    HttpResponse::Ok().body(value)
}

#[delete("/delete/{key}")]
async fn delete_handler(eng: web::Data<Arc<Engine>>, key: web::Path<String>) -> impl Responder {
    if let Err(e) = eng.del(key.as_bytes()) {
        if matches!(e, Errors::KeyEmpty) {
            return HttpResponse::InternalServerError()
                .body(format!("Failed to delete value in engine, due to:\n{}", e));
        }
    }
    HttpResponse::Ok().body("OK")
}

#[get("/state")]
async fn stat_handler(eng: web::Data<Arc<Engine>>) -> impl Responder {
    let stat = eng.state();

    let result: HashMap<_, _> = stat.into();
    HttpResponse::Ok().body(serde_json::to_string(&result).unwrap())
}

#[post("/merge")]
async fn merge_handler(eng: web::Data<Arc<Engine>>) -> impl Responder {
    spawn(async move {
        let _ = eng.merge();
    });

    HttpResponse::Ok().body("Enging is merging")
}
