use anyhow::Result;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use db_engine::database::{Database, DatabaseBuilder};

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Mutex<Database>>,
}

impl AppState {
    pub fn new() -> Result<Self> {
        let db_dir_path = PathBuf::from("./db");
        let db = DatabaseBuilder::new(db_dir_path)?.build();

        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }
}
