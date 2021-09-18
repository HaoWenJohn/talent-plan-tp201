use crate::{KvsEngine,Result,Error};
use std::path::PathBuf;
use sled::{open,Db};
///for benchmark
pub struct SledKvsEngine{
    db:Db
}
impl SledKvsEngine{
    ///open
    pub fn open(path: impl Into<PathBuf> + Clone) -> Result<Self> {
        let db:Db = sled::open(path.into()).unwrap();
        Ok(SledKvsEngine{
            db
        })
    }
}

impl KvsEngine for SledKvsEngine{

    fn set(&mut self, key: String, value: String) -> Result<()>{
        self.db.insert(key.as_bytes(),value.as_bytes()).unwrap();
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>>{
        return match self.db.get(&key.as_bytes()).unwrap(){
            None=>{
                Ok(None)
            },
            Some(i_vec)=>{
                Ok(Some(String::from_utf8(i_vec.as_ref().to_owned())?))
            }
        }
    }

    fn remove(&mut self, key: String) -> Result<()>{
        return match self.db.remove(&key.as_bytes()).unwrap(){
            None=> Err(Error::KeyNotFoundError),
            Some(_)=>Ok(())
        }
    }
}
#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::err::{Result, Error};
    use crate::{SledKvsEngine, KvsEngine};

    use std::path::Path;

    #[test]
    fn test_open() -> Result<()> {
        let tmp = TempDir::new().expect("create new dir err");
        let db = SledKvsEngine::open(tmp.path())?;
        drop(db);
        Ok(())
    }

    #[test]
    fn test_set() -> Result<()> {
        let tmp = TempDir::new().expect("create new dir err");
        let mut db = SledKvsEngine::open(tmp.path())?;
        db.set("key1".to_owned(), "value1".to_owned());

        Ok(())
    }

    #[test]
    fn test_get() -> Result<()> {
        let tmp = TempDir::new().expect("create new dir err");
        let mut db = SledKvsEngine::open(tmp.path())?;
        db.set("key1".to_owned(), "value1".to_owned())?;
        db.set("key1".to_owned(), "value2".to_owned())?;
        assert_eq!(db.get("key1".to_owned())?, Some("value2".to_owned()));
        Ok(())

    }

    #[test]
    fn test_remove() -> Result<()> {
        let tmp = TempDir::new().expect("create new dir err");
        let mut db = SledKvsEngine::open(tmp.path())?;
        db.set("key1".to_owned(), "value1".to_owned())?;
        db.remove("key1".to_owned())?;

        assert_eq!(db.get("key1".to_owned())?, None);

        Ok(())
    }


}

