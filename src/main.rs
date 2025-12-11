use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

fn main() {
    let input_path = std::env::args()
        .nth(1)
        .expect("Usage: obrc-rust [input_file] [output_file]");
    let output_path = std::env::args()
        .nth(2)
        .expect("Usage: obrc-rust [input_file] [output_file]");
    let brc_reader = BrcReader::new(input_path.into());
    let tree = brc_reader.run_naive();
    BrcReader::write_map(&tree, &PathBuf::from(output_path));
}

struct BrcReader {
    file_path: PathBuf,
}

impl BrcReader {
    pub fn new(path: PathBuf) -> BrcReader {
        BrcReader { file_path: path }
    }

    pub fn run_naive(&self) -> BTreeMap<String, (f32, f32, f32, i32)> {
        //                             min  mean max count
        let mut map: BTreeMap<String, (f32, f32, f32, i32)> = BTreeMap::new();

        let file = File::open(&self.file_path).unwrap();
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(0)).unwrap();

        for val in reader.lines() {
            let line = val.unwrap();
            let v = line.split(';').collect::<Vec<&str>>();
            let key = String::from(v[0]);
            let value = String::from(v[1]).parse::<f32>().unwrap();

            if let Some(val) = map.get_mut(&key) {
                let new_count = (*val).3 + 1;

                // Check for min
                if (*val).0 > value {
                    (*val).0 = value;
                }

                // Check for mean
                let new_mean = ((*val).1 * (*val).3 as f32 + value) / new_count as f32;
                (*val).1 = new_mean;
                (*val).3 = new_count;

                // Check for max
                if (*val).2 < value {
                    (*val).2 = value;
                }
            } else {
                map.insert(key, (value, value, value, 1));
            }
        }

        map
    }

    pub fn run_partitioned(&self, partitions: u8) -> BTreeMap<String, (f32, f32, f32, i32)> {
        let file = File::open(&self.file_path).unwrap();
        let metadata = file.metadata().unwrap();
        let file_size = metadata.len();
    }

    fn merge(
        vec: Vec<BTreeMap<String, (f32, f32, f32, i32)>>,
    ) -> BTreeMap<String, (f32, f32, f32, i32)> {
        let mut map: BTreeMap<String, (f32, f32, f32, i32)> = BTreeMap::new();

        for tree in vec {
            for (key, value) in tree {}
        }

        map
    }

    fn spawn_worker(
        buf_reader: &mut BufReader<File>,
        partition_n: u8,
    ) -> Arc<BTreeMap<String, (f32, f32, f32, i32)>> {
        // TODO: Make BtreeMap and BufReader thread safe
        //                             min  mean max count
        let mut map: Arc<BTreeMap<String, (f32, f32, f32, i32)>> = Arc::new(BTreeMap::new());
        let handler = std::thread::spawn(|| {
            map.insert("a".to_string(), (0.0, 0.0, 0.0, 0));
        });

        handler.join().unwrap();

        map
    }

    pub fn write_map(map: &BTreeMap<String, (f32, f32, f32, i32)>, path: &PathBuf) {
        let file = File::create(path).unwrap();
        let mut writer = BufWriter::new(file);
        for (city, (min, mean, max, _count)) in map {
            writeln!(writer, "{};{:.9};{:.9};{:.9}", city, min, mean, max).unwrap();
        }
        writer.flush().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::BrcReader;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn basic_parse_test() {
        let mut temp_file = NamedTempFile::new().unwrap();

        let test_data = "\
Tokyo;10.0
Tokyo;20.0
London;5.0
Paris;50.0
London;15.0
Paris;100.0
Sydney;-5.0
Paris;75.0
Berlin;-10.5
Berlin;10.5
NewYork;25.0
Berlin;0.0
Sydney;5.0
Sydney;0.0
Tokyo;30.0
";

        // Expected results:
        // Berlin=min(-10.5), mean(0.0), max(10.5)
        // London=min(5.0), mean(10.0), max(15.0)
        // NewYork=min(25.0), mean(25.0), max(25.0)
        // Paris=min(50.0), mean(75.0), max(100.0)
        // Sydney=min(-5.0), mean(0.0), max(5.0)
        // Tokyo=min(10.0), mean(20.0), max(30.0)

        let sorted_list = vec!["Berlin", "London", "NewYork", "Paris", "Sydney", "Tokyo"];
        let outputs = vec![
            (-10.5, 0.0, 10.5),
            (5.0, 10.0, 15.0),
            (25.0, 25.0, 25.0),
            (50.0, 75.0, 100.0),
            (-5.0, 0.0, 5.0),
            (10.0, 20.0, 30.0),
        ];

        temp_file.write_all(test_data.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        let reader = BrcReader::new(temp_file.path().into());
        let output = reader.run_naive();
        assert_eq!(output.len(), outputs.len());
        for (i, (k, v)) in output.iter().enumerate() {
            assert_eq!(k, sorted_list[i]);
            assert_eq!((*v).0, outputs[i].0);
            assert_eq!((*v).1, outputs[i].1);
            assert_eq!((*v).2, outputs[i].2);
        }
    }
}
