use std::{error::Error, num::NonZeroU16, path::PathBuf};

use bitcask_rs_core::config::Config;
use clap::{Parser, Subcommand};

#[derive(Parser)]
pub struct Args {
    #[command(subcommand)]
    pub commands: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run the bitcask server
    Serve(Serve),
    /// Use the builtin shell to interact with the bitcask server
    Shell,
}

#[derive(Parser)]
pub struct Serve {
    #[arg(short, long, default_value = "8888")]
    /// The port that the bitcask server will listen
    pub port: NonZeroU16,
    #[clap(long)]
    /// Use the default bitcask config
    default_config: bool,
    #[clap(long)]
    /// The bitcask server config path
    config: Option<PathBuf>,
}

impl Serve {
    pub async fn read_config(&self) -> Result<Config, Box<dyn Error>> {
        if let Some(config_path) = self.config.as_ref() {
            if let Some(ext_name) = config_path.extension() {
                if ext_name == "json" {
                    todo!()
                }
                if ext_name == "toml" {
                    todo!()
                }

                return Err("unsupported file format".into());
            }
        }

        if self.default_config {
            Ok(Config::default())
        } else {
            Err("should privide config".into())
        }
    }
}
