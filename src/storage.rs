use crate::error::Result;
use crate::models::Kline;
use csv::Writer;
use log::{debug, info};
use std::fs::File;
use std::path::Path;

/// Storage handler for kline data
pub struct Storage;

impl Storage {
    /// Save klines to a CSV file
    pub fn save_klines_to_csv<P: AsRef<Path>>(
        klines: &[Kline],
        path: P,
        append: bool,
    ) -> Result<()> {
        let path = path.as_ref();
        let file_exists = path.exists();

        debug!(
            "{} klines to {} at {}",
            if append { "Appending" } else { "Writing" },
            klines.len(),
            path.display()
        );

        let file = File::options()
            .write(true)
            .create(true)
            .append(append)
            .truncate(!append)
            .open(path)?;

        let mut writer = Writer::from_writer(file);

        // Write headers if the file is new or we're not appending
        if !file_exists || !append {
            writer.write_record(&Kline::csv_headers())?;
        }

        // Write kline data
        for kline in klines {
            writer.write_record(&kline.to_csv_record())?;
        }

        writer.flush()?;

        info!(
            "Successfully {} {} klines to {}",
            if append { "appended" } else { "wrote" },
            klines.len(),
            path.display()
        );

        Ok(())
    }

    /// Check if a file exists and get its size
    #[allow(dead_code)]
    pub fn get_file_size<P: AsRef<Path>>(path: P) -> Option<u64> {
        let path = path.as_ref();
        if path.exists() {
            if let Ok(metadata) = std::fs::metadata(path) {
                return Some(metadata.len());
            }
        }
        None
    }
}
