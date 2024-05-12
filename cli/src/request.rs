use serde::Deserialize;

#[derive(Deserialize)]
pub struct GetBody {
    pub key: String,
}

#[derive(Deserialize)]
pub struct PutBody {
    pub key: String,
    pub value: String,
}
