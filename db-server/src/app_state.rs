use anyhow::{Context, Result};
use std::{fs::create_dir_all, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

use db_engine::{Database, DatabaseBuilder};

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Mutex<Database>>,
}

impl AppState {
    pub async fn new() -> Result<Self> {
        let db_dir_path = PathBuf::from("./db");
        create_dir_all(&db_dir_path).context("create db dir")?;

        let db_engine = DatabaseBuilder::new(db_dir_path).await?.build();
        let db = Arc::new(Mutex::new(db_engine));

        Ok(Self { db })
    }
}
