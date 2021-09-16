use structopt::{StructOpt};
use kvs::Database;
use std::process::exit;

#[derive(StructOpt)]
#[structopt(name = "kv-database",
about = "this is hw for pincap talent-plan course TP 201: Practical Networked Applications in Rust")]
struct Opt {
    #[structopt(subcommand)]
    sub_opt: SubOpt,
}

#[derive(StructOpt)]
enum SubOpt {
    #[structopt(name = "set", about = "store key-value mapping ")]
    Set {
        key: String,
        value: String,
    },
    #[structopt(name = "get", about = "get stored value by given key")]
    Get {
        key: String
    },
    #[structopt(name = "rm", about = "remove stored value if given key exists")]
    Remove {
        key: String
    },
}

fn main() {

    let opt = Opt::from_args();
    let mut kvs = Database::open("./").unwrap();
    match opt.sub_opt {
        SubOpt::Set { key, value } => {
            kvs.set(key, value);
        }
        SubOpt::Get { key } => {
            if let Ok(Some(value)) = kvs.get(key.clone()){
                println!("{}",value);
            }else {
                println!("Key not found");
            }
        }
        SubOpt::Remove { key } => {
            match kvs.remove(key){
                Ok(_) => {}
                Err(err) => {
                    println!("Key not found");
                    exit(1);
                }
            }
        }
    }
}
