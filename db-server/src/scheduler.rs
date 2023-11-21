use std::path::PathBuf;

use db_engine::Compaction;

pub struct Scheduler {
    db_dir_path: PathBuf,
    compact_limit: u64,
    file_ext: String,
}

impl Scheduler {
    pub fn new(db_dir: &str, compact_limit: u64) -> Self {
        Self {
            db_dir_path: PathBuf::from(db_dir),
            compact_limit,
            file_ext: "db".to_string(),
        }
    }

    pub async fn perform(&self) {
        tracing::info!("Start scheduler to compact the database");

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;

            tracing::info!("Start compacting the database");
            let db_compaction = Compaction::new(
                self.db_dir_path.clone(),
                self.compact_limit,
                self.file_ext.as_str(),
            );
            if let Err(e) = db_compaction.compact().await {
                tracing::error!("Error while compacting: {}", e);
            }
        }
    }
}
