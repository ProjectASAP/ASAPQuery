use crate::data_model::{AggregateCore, PrecomputedOutput};
use serde::Serialize;
use std::fs::{create_dir_all, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info};

#[derive(Serialize)]
struct PrecomputeDump {
    timestamp: u64,
    metadata: PrecomputedOutput,
    accumulator_type: String,
    accumulator_data_bytes: Vec<u8>,
}

pub struct PrecomputeDumper {
    file: BufWriter<File>,
    dump_count: u64,
    file_path: String,
}

impl PrecomputeDumper {
    pub fn new(output_dir: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Create precompute_dumps subdirectory
        let dump_dir = Path::new(output_dir).join("precompute_dumps");
        create_dir_all(&dump_dir)?;

        // Generate filename with timestamp
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let filename = format!("precomputes_{timestamp}.msgpack");
        let file_path = dump_dir.join(filename);

        let file = File::create(&file_path)?;
        let buffered_writer = BufWriter::new(file);

        info!("Created precompute dump file: {:?}", file_path);

        Ok(Self {
            file: buffered_writer,
            dump_count: 0,
            file_path: file_path.to_string_lossy().to_string(),
        })
    }

    pub fn dump_precompute(
        &mut self,
        output: &PrecomputedOutput,
        accumulator: &dyn AggregateCore,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Create the dump record
        let dump = PrecomputeDump {
            timestamp,
            metadata: output.clone(),
            accumulator_type: accumulator.type_name().to_string(),
            accumulator_data_bytes: accumulator.serialize_to_bytes(),
        };

        // Serialize to MessagePack
        let serialized_data = rmp_serde::to_vec(&dump)
            .map_err(|e| format!("Failed to serialize precompute dump: {e}"))?;

        // Write length prefix (4 bytes, little-endian)
        let length = serialized_data.len() as u32;
        self.file.write_all(&length.to_le_bytes())?;

        // Write the serialized data
        self.file.write_all(&serialized_data)?;

        self.dump_count += 1;

        debug!(
            "Dumped precompute #{}: type={}, aggregation_id={}, size={} bytes",
            self.dump_count,
            dump.accumulator_type,
            output.aggregation_id,
            serialized_data.len()
        );

        // Flush every 100 records to ensure data is written
        if self.dump_count.is_multiple_of(100) {
            self.file.flush()?;
            debug!(
                "Flushed precompute dump file after {} records",
                self.dump_count
            );
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.file.flush()?;
        debug!(
            "Flushed precompute dump file with {} total records",
            self.dump_count
        );
        Ok(())
    }

    pub fn get_dump_count(&self) -> u64 {
        self.dump_count
    }

    pub fn get_file_path(&self) -> &str {
        &self.file_path
    }
}

impl Drop for PrecomputeDumper {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            error!("Failed to flush precompute dump file on drop: {}", e);
        } else {
            info!(
                "Closed precompute dump file {} with {} records",
                self.file_path, self.dump_count
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::precompute_operators::SumAccumulator;
    use tempfile::TempDir;

    #[test]
    fn test_precompute_dumper_creation() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().to_str().unwrap();

        let dumper = PrecomputeDumper::new(output_dir);
        assert!(dumper.is_ok());

        let dumper = dumper.unwrap();
        assert_eq!(dumper.get_dump_count(), 0);
        assert!(dumper.get_file_path().contains("precomputes_"));
        assert!(dumper.get_file_path().ends_with(".msgpack"));
    }

    #[test]
    fn test_precompute_dumping() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().to_str().unwrap();

        let mut dumper = PrecomputeDumper::new(output_dir).unwrap();

        // Create test precompute data
        let accumulator = SumAccumulator::with_sum(42.5);
        let output = PrecomputedOutput {
            start_timestamp: 1000,
            end_timestamp: 2000,
            key: None,
            aggregation_id: 1,
        };

        // Dump the precompute
        let result = dumper.dump_precompute(&output, &accumulator);
        assert!(result.is_ok());
        assert_eq!(dumper.get_dump_count(), 1);

        // Test flushing
        let flush_result = dumper.flush();
        assert!(flush_result.is_ok());
    }
}
