extern crate redis;
extern crate serde_json;
extern crate rs_es;
use std::thread;
use std::io;
use redis::{Client, Commands};
use std::error::Error;
use std::io::{Write, stderr};
use serde_json::Value;
use rs_es::operations::bulk::Action;

static NTHREADS: i32 = 4;
static NITEMS: usize = 50;

/// Dump an error message to `stderr`.
///
/// If another error happens while building the error message or
/// writing to `stderr`, it is ignored.
fn print_error(mut err: &Error) {
    let _ = writeln!(stderr(), "error: {}", err);
    while let Some(cause) = err.cause() {
        let _ = writeln!(stderr(), "caused by: {}", cause);
        err = cause;
    }
}

//fn post_actions<T>(es_client: rs_es::Client, actions: Vec<T>){
//    println!("Posting would happen now")
//}

fn run() -> io::Result<()> {

    // threads
    println!("Making threads");
    let mut children = vec![];
    for i in 0..NTHREADS{

        children.push (thread::spawn(move || {
            // setup the thread with connections
            let redis_client = Client::open("redis://127.0.0.1/").unwrap();
            let conn = redis_client.get_connection().unwrap();
            let mut es_client = rs_es::Client::new("http://localhost:9200").unwrap();

            // a place to hold our records until we are ready to bulk insert
            let mut actions = vec![];
            let mut json = String::from("");
            // read until empty
            while !json.starts_with("nil"){
                json = conn.lpop("eventqueue").unwrap_or("nil".to_string());
                if json.starts_with("nil") {
                    //println!("Thread {}, Nothing left to do",i);
                    if actions.len() > 0{
                        let _ = es_client.bulk(&actions).send();
                        println!("Thread {}, last bulk finished",i);
                        actions.clear();
                    }else{
                        println!("Thread {}, finished empty",i);
                    }
                }
                else{

                    //println!("thread {}, raw json {:?}",i,json);
                    let v: Value = serde_json::from_str(&json).unwrap();
                    //println!("thread {}, json.summary {}",i,v["summary"]);
                    actions.push(Action::index(v).with_index("events").with_doc_type("rustevent"));
                    //println!("thread {}, actions length: {:?}",i, actions.len());
                    if actions.len() >= NITEMS {
                        let _ = es_client.bulk(&actions).send();
                        //println!("Thread {}, bulk finished",i);
                        //println!("INDEX RESULT: {:?}", result_wrapped);
                        actions.clear();
                    }

                    //let result_wrapped = es_client
                    //                    .index("events", "rustevent")
                    //                    .with_doc(&v)
                    //                    .send();

                    //println!("INDEX RESULT: {:?}", result_wrapped);
                }
            }
        }));
    }

    for child in children{
        let _ = child.join();
    }

    Ok(())
}

fn main() {
    if let Err(err) = run() {
        print_error(&err);
        std::process::exit(1);
    }
}
