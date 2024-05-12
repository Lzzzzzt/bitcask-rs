use std::{error::Error, net::SocketAddr, sync::Arc};

use axum::{
    routing::{get, post},
    Router,
};
use bitcask::{
    args::{Args, Commands},
    handler::{bitcask_get, bitcask_put},
};
use bitcask_rs_core::db::Engine;
use clap::Parser;
use log::info;

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    match args.commands {
        Commands::Shell => {}
        Commands::Serve(server_config) => {
            env_logger::init();

            let config = server_config.read_config().await?;

            let addr = SocketAddr::from(([0, 0, 0, 0], server_config.port.get()));

            let engine = Arc::new(Engine::open(config)?);

            let server = Router::new()
                .route("/get", get(bitcask_get))
                .route("/put", post(bitcask_put))
                .with_state(engine);

            info!("Server will listening on: {:?}", addr);

            axum::serve(TcpListener::bind(addr).await?, server).await?
        }
    }

    Ok(())
}
