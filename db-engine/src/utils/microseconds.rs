use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

pub fn micros_now() -> Result<u128> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("generate new timestamp")?
        .as_micros();
    Ok(timestamp)
}
