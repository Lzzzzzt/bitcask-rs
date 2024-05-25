use std::sync::Arc;

use actix_web::{web, App, HttpServer, Scope};
use bitcask_http::*;
use bitcask_rs_core::{config::Config, db::Engine};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // 启动 Engine 实例
    let opts = Config::default();

    let engine = Arc::new(Engine::open(opts).unwrap());

    // 启动 http 服务
    HttpServer::new(move || {
        App::new().app_data(web::Data::new(engine.clone())).service(
            Scope::new("/bitcask")
                .service(put_handler)
                .service(get_handler)
                .service(delete_handler)
                .service(stat_handler)
                .service(merge_handler),
        )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
