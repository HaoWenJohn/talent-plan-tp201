use structopt::{StructOpt};
use Kvs::{Result};
use std::process::exit;
use std::net::{TcpStream, SocketAddr};
use Kvs::utils::{parse_addr, Command};
use std::io::{Write, Read};
use serde_json::error::Category::Eof;

#[derive(Debug,StructOpt)]
#[structopt(name = "kvs-client",
about = "this is hw for pincap talent-plan course TP 201: Practical Networked Applications in Rust")]
struct Opt {

    #[structopt(subcommand)]
    sub_opt: SubOpt,
}

#[derive(Debug,StructOpt)]
pub enum SubOpt {
    #[structopt(name = "set", about = "store key-value mapping ")]
    Set {
        key: String,
        value: String,
        #[structopt(long, default_value = "127.0.0.1:4000", parse(try_from_str = parse_addr))]
        addr: SocketAddr,
    },
    #[structopt(name = "get", about = "get stored value by given key")]
    Get {
        key: String,
        #[structopt(long, default_value = "127.0.0.1:4000", parse(try_from_str = parse_addr))]
        addr: SocketAddr,
    },
    #[structopt(name = "rm", about = "remove stored value if given key exists")]
    Remove {
        key: String,
        #[structopt(long, default_value = "127.0.0.1:4000", parse(try_from_str = parse_addr))]
        addr: SocketAddr,
    },
}

fn main() ->Result<()>{
    let opt = Opt::from_args();
    let (cmd,addr)=match opt.sub_opt{
        SubOpt::Set{ key,value,addr }=>{
            (Command::Set(key,value),addr)

        },
        SubOpt::Get{key,addr}=>{
            (Command::Get(key),addr)
        },
        SubOpt::Remove {key,addr}=>{
            (Command::Remove(key),addr)
        }
    };
    let mut stream =  TcpStream::connect(addr).expect("connect error");
    serde_json::ser::to_writer(stream.try_clone()?,&cmd)?;
    let cmd:Command = serde_json::from_reader(stream)?;
    match cmd {
        Command::Pong => {
            println!("Pong!");
        }
        Command::Ok(res) => {
            if let Some(str)=res{
                println!("{}",str);
            }else { println!("Ok") }
        }
        Command::Err => {
            println!("Err!");
        }
        _ => {}
    }

    Ok(())
}
