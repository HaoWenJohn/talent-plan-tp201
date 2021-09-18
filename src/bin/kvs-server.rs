use structopt::{StructOpt};
use Kvs::{KvStore, Result, Error, KvsEngine,SledKvsEngine};
use std::process::exit;
use std::path::PathBuf;
use std::net::{ToSocketAddrs, SocketAddr, AddrParseError, TcpListener};
use std::str::FromStr;
use std::io::{Read, Write};
use Kvs::utils::{parse_addr, Command};
use serde::Deserialize;

#[derive(Debug, StructOpt)]
#[structopt(name = "Kvs-server",
about = "this is hw for pincap talent-plan course TP 201: Practical Networked Applications in Rust")]
struct Opt {
    #[structopt(long, default_value = "127.0.0.1:4000", parse(try_from_str = parse_addr))]
    addr: SocketAddr,

    #[structopt(long)]
    engine: Option<String>,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let listener = TcpListener::bind(opt.addr)?;
    let kvs_exist =  PathBuf::new().join("./.data").is_file() ;
    let sled_exist = PathBuf::new().join("./db").is_file() ;
    let engine:Result<Box<dyn KvsEngine>>= match opt.engine {
        Some(engine)=>{
             if engine=="kvs".to_owned()&&!sled_exist{
                 Ok(Box::new(KvStore::open(".")?))
            }else if engine=="sled".to_owned()&&!kvs_exist {
                Ok(Box::new(SledKvsEngine::open(".")?))
            }else {
                Err(Error::InvalidEngineError)
            }
        },
        None=>{
            if sled_exist {
                 Ok(Box::new(SledKvsEngine::open(".")?))
            }else {
                Ok(Box::new(KvStore::open(".")?))
            }

        }
    };
    let mut engine = engine.unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {

                let mut de = serde_json::Deserializer::from_reader(stream.try_clone()?);
                let cmd :Command=Command::deserialize(&mut de) ?;

                match cmd {
                    Command::Set(k, v) => {
                        println!("0");
                        engine.set(k, v)?;
                        serde_json::to_writer(stream.try_clone()?,&Command::Ok(None));
                        println!("1");
                    }
                    Command::Get(k) => {
                        println!("2");
                        let v = engine.get(k)?;
                        match v {
                            None => {  serde_json::to_writer(stream.try_clone()?,&Command::Ok(None));}
                            Some(str) => {  serde_json::to_writer(stream.try_clone()?,&Command::Ok(Some(str))); }
                        };
                        println!("3");
                    }
                    Command::Remove(k) => {
                        if let Ok(()) = engine.remove(k) {
                            serde_json::to_writer(stream.try_clone()?,&Command::Ok(None));
                        } else {  serde_json::to_writer(stream.try_clone()?,&Command::Err);};

                    },
                    Command::Ping=>{
                        serde_json::to_writer(stream.try_clone()?,&"Pong")?;
                    }
                    _ => {}
                }

            }
            Err(_) => {
                return Err(Error::ConnectFailedError);
            }
        }
    }

    Ok(())
}
