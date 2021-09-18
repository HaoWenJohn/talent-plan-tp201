use crate::err::{Result, Error};
use std::collections::{HashMap, BTreeMap};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use std::fs::{OpenOptions, File};
use crate::kvs::utils::open_file;
use std::io::{BufReader, BufWriter, Write, Seek, SeekFrom, Read, BufRead};
use std::iter::FromIterator;
use crate::KvsEngine;

const DATA_FILE_SIZE: i32 = 1 << 22;
const COMPACT_THRESHOLD: i32 = 1 << 21;

///A key-value database based on log structure,[bitcast](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf)
/// is referred to.It append data to logfile and update the index in memory.when a large amount of data is out of date,
/// logfile will be compressed.
pub struct Database {
    dir: PathBuf,
    index: BTreeMap<String, Index>,
    file: File,
    writer: BufWriter<File>,
    reader: BufReader<File>,
    outdated_len: usize,
}
impl KvsEngine for Database{
    ///inset a key-value mapping into database,it write data to disk firstly,then record the physical
    ///position in memory
    fn set(&mut self, key: String, value: String) -> Result<()> {
        if self.outdated_len >= COMPACT_THRESHOLD as usize {
            self.compact()?;
        }
        let text = serde_json::to_string(&Log(key.clone(), Some(value)))?;
        let (start, len) = append_new_content(&mut self.writer, text)?;
        match self.index.insert(key.clone(), Index {
            key,
            start,
            end: start + len,
        }) {
            None => {}
            Some(index) => {
                self.outdated_len += index.end - index.start;
            }
        }
        self.writer.flush()?;

        Ok(())
    }


    ///query data by given key
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let value = match self.index.get(&key) {
            None => None,
            Some(index) => {
                //self.file.seek(SeekFrom::Start(index.start as u64))?;
                let raw_string = read_by_pos(&mut self.reader, index.start, index.end)?;
                let log: Log = serde_json::from_str(raw_string.as_str())?;
                //let mut de = serde_json::Deserializer::from_reader(BufReader::new(self.file.try_clone()?));
                //let log = Log::deserialize(&mut de)?;
                Some(log.1.unwrap())
            }
        };
        Ok(value)
    }

    ///remove data by given key
    fn remove(&mut self, key: String) -> Result<()> {
        let content = serde_json::to_string(&Log(key.clone(), None))?;
        return match self.index.remove(&key) {
            None => Err(Error::KeyNotFoundError),
            Some(index) => {
                let (_,len)=append_new_content(&mut self.writer, content)?;
                self.outdated_len += index.end - index.start +len;
                Ok(())
            }
        };
    }
}
impl Database {
    ///creating a new instance by given log dir
    pub fn open(path: impl Into<PathBuf> + Clone) -> Result<Self> {
        let file = open_file(path.clone(), true, ".data")?;
        let reader = BufReader::new(file.try_clone()?);
        let mut outdated_len = 0;
        let idxs = serde_json::Deserializer::from_reader(reader)
            .into_iter::<Log>()

            .map(|pair| pair.unwrap())
            //so ugly
            .map(|log| (log.0.clone(), serde_json::to_string(&log).unwrap().len(), log.1.is_none()))

            .collect::<Vec<(String, usize, bool)>>();
        let mut map: BTreeMap<String, Index> = BTreeMap::new();
        map_adjacent(0, idxs)
            .into_iter()
            .for_each(|(index, delete)|
                if delete {
                    let removed_data = map.remove(&index.key).unwrap();
                    outdated_len += index.end - index.start + removed_data.end - removed_data.start;
                } else {
                    map.insert(index.key.clone(), index);
                }
            );
        Ok(Database {
            dir: path.into(),
            index: map,
            file: file.try_clone()?,
            writer: BufWriter::new(file.try_clone()?),
            reader: BufReader::new(file.try_clone()?),
            outdated_len,
        })
    }





    fn compact(&mut self) -> Result<()> {
        let new_file = open_file(&self.dir, true, ".data_tmp")?;
        let mut new_index = BTreeMap::new();
        let mut new_writer = BufWriter::new(new_file.try_clone()?);
        let mut old_reader = BufReader::new(self.file.try_clone()?);
        self.index.values()
            .map(|index| (index.key.clone(), read_by_pos(&mut old_reader, index.start, index.end)))
            .map(|strs| (strs.0, append_new_content(&mut new_writer, strs.1.unwrap()).unwrap()))
            .for_each(|(key, (start, len))| drop(new_index.insert(key.clone(), Index {
                key,
                start,
                end: start + len,
            })));
        self.index = new_index;
        self.writer = new_writer;
        self.writer.flush()?;
        self.reader = BufReader::new(new_file.try_clone()?);
        self.file = new_file;
        self.outdated_len = 0;
        std::fs::rename(self.dir.join(".data_tmp"), self.dir.join(".data"))?;

        Ok(())
    }
}

///append content to file,return start pos and len
fn append_new_content(writer: &mut BufWriter<File>, content: String) -> Result<(usize, usize)> {
    let start = writer.seek(SeekFrom::End(0))? as usize;
    let len = writer.write(content.as_bytes())?;
    writer.flush()?;
    Ok((start, len))
}

fn read_by_pos(reader: &mut BufReader<File>, start: usize, end: usize) -> Result<String> {
    let len = end - start;
    let mut buffer = vec![0; len];
    reader.seek(SeekFrom::Start(start as u64))?;
    reader.read_exact(buffer.as_mut_slice())?;
    Ok(String::from_utf8(buffer)?)
}

///(key,length,is_deleted)->(key,start,end,is_deleted)
fn map_adjacent(start_pos: usize, collection: Vec<(String, usize, bool)>) -> Vec<(Index, bool)> {
    return match collection.split_first() {
        None => vec![],
        Some(((str, len, delete), tail)) =>
            vec![vec![(Index { key: str.to_string(), start: start_pos, end: start_pos + len }, delete.clone())], map_adjacent(start_pos + len, tail.to_vec())].concat()
    };
}


///it points to where data is stored in disk
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Index {
    key: String,
    start: usize,
    end: usize,
}

///data stored in disk,log(key,value)
#[derive(Debug, Serialize, Deserialize)]
struct Log(String, Option<String>);

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::err::{Result, Error};
    use crate::{ KvsEngine, KvStore};
    use crate::kvs::database::{Index, Log};
    use std::path::Path;

    #[test]
    fn test_open() -> Result<()> {
        let tmp = TempDir::new().expect("create new dir err");
        let db = KvStore::open(tmp.path())?;
        drop(db);
        Ok(())
    }

    #[test]
    fn test_set() -> Result<()> {
        let tmp = TempDir::new().expect("create new dir err");
        let mut db = KvStore::open(tmp.path())?;
        db.set("key1".to_owned(), "value1".to_owned());
        let content = Log("key1".to_owned(), Some("value1".to_owned()));
        let len = serde_json::to_string(&content)?.len();
        let stored_data = db.index.get("key1").cloned().unwrap();

        assert_eq!(stored_data.key, "key1".to_owned());
        assert_eq!(stored_data.start, 0);
        assert_eq!(stored_data.end, len);

        Ok(())
    }

    #[test]
    fn test_get() -> Result<()> {
        let tmp = TempDir::new().expect("create new dir err");
        let mut db = KvStore::open(tmp.path())?;
        db.set("key1".to_owned(), "value1".to_owned())?;
        db.set("key1".to_owned(), "value2".to_owned())?;
        assert_eq!(db.get("key1".to_owned())?, Some("value2".to_owned()));


        drop(db);
        let mut db = KvStore::open(tmp.path())?;
        let res = db.get("key1".to_owned())?;
        assert_eq!(db.get("key1".to_owned())?, Some("value2".to_owned()));
        Ok(())
    }

    #[test]
    fn test_remove() -> Result<()> {
        let tmp = TempDir::new().expect("create new dir err");
        let mut db = KvStore::open(tmp.path())?;
        db.set("key1".to_owned(), "value1".to_owned())?;
        db.remove("key1".to_owned())?;

        assert_eq!(db.get("key1".to_owned())?, None);
        assert!(db.remove("key1".to_owned()).is_err());
        assert!(db.remove("key2".to_owned()).is_err());
        drop(db);
        let mut db = KvStore::open(tmp.path())?;
        assert!(db.remove("key1".to_owned()).is_err());

        Ok(())
    }

    #[test]
    fn test_compaction() -> Result<()> {
        let tmp = TempDir::new().expect("create new dir err");
        let mut db = KvStore::open(tmp.path())?;

        db.set("key1".to_owned(), "value1".to_owned());
        db.set("key1".to_owned(), "value2".to_owned());

        db.set("key2".to_owned(), "value2".to_owned());
        db.remove("key2".to_owned());

        db.compact()?;
        assert_eq!(db.index.len(), 1);
        assert_eq!(db.get("key1".to_owned())?, Some("value2".to_owned()));
        assert_eq!(db.get("key2".to_owned())?, None);
        Ok(())
    }
}

