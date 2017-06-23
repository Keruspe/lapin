extern crate tokio_core;
extern crate amq_protocol;
extern crate futures;
extern crate env_logger;
extern crate lapin_futures as lapin;

use amq_protocol::types::FieldTable;
use futures::{Future, Sink, Stream};
use futures::stream::BoxStream;
use futures::sync::mpsc::channel;
use lapin::client::{Client, ConnectionOptions};
use lapin::channel::{BasicPublishOptions,QueueDeclareOptions, BasicProperties};
use std::io::{self, BufRead};
use std::thread;
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;

fn stdin() -> BoxStream<String, io::Error> {
    let (mut tx, rx) = channel(1);
    thread::spawn(move || {
        let input = io::stdin();
        for line in input.lock().lines() {
            match tx.send(line).wait() {
                Ok(s) => tx = s,
                Err(_) => break,
            }
        }
    });
    rx.then(|e| e.unwrap()).boxed()
}

fn main() {
  env_logger::init().unwrap();
  let mut core = Core::new().unwrap();
  let handle = core.handle();
  let addr = "127.0.0.1:5672".parse().unwrap();

  core.run(
    TcpStream::connect(&addr, &handle).and_then(|stream| Client::connect(stream, &ConnectionOptions::default()))
    .and_then(|client| client.create_channel())
    .and_then(|channel|
      channel.queue_declare("test", &QueueDeclareOptions::default(), &FieldTable::new())
      .and_then(move |_|
        stdin().for_each(move |string| {
          println!("Sending: {}", string);
          channel.basic_publish("", "test", string.as_bytes(), &BasicPublishOptions::default(), BasicProperties::default())
          .map(|_|())
        })
      )
    )
  ).unwrap();
}
