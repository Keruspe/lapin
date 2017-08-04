extern crate lapin_futures as lapin;
extern crate futures;
extern crate tokio_core;
extern crate env_logger;

use futures::future::Future;
use futures::Stream;
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use lapin::types::FieldTable;
use lapin::client::ConnectionOptions;
use lapin::channel::{BasicConsumeOptions,BasicPublishOptions,BasicProperties,ExchangeBindOptions,ExchangeUnbindOptions,ExchangeDeclareOptions,ExchangeDeleteOptions,QueueBindOptions,QueueDeclareOptions};
use std::thread;

fn main() {
    env_logger::init().unwrap();
    let mut core = Core::new().unwrap();

    let handle = core.handle();
    let addr = "127.0.0.1:5672".parse().unwrap();

    core.run(
        TcpStream::connect(&addr, &handle).and_then(|stream| {
            lapin::client::Client::connect(stream, &ConnectionOptions {
                username: "guest".to_string(),
                password: "guest".to_string(),
                frame_max: 65535,
                ..Default::default()
            })
        }).and_then(|(client, heartbeat_future_fn)| {
            let heartbeat_client = client.clone();
            thread::Builder::new().name("heartbeat thread".to_string()).spawn(move || {
                Core::new().unwrap().run(heartbeat_future_fn(&heartbeat_client)).unwrap();
            }).unwrap();

            client.create_channel().and_then(|channel| {
                let id = channel.id;
                println!("created channel with id: {}", id);

                let ack_channel = channel.clone();

                channel.queue_declare("hello", &QueueDeclareOptions::default(), &FieldTable::new()).and_then(move |_| {
                    println!("channel {} declared queue {}", id, "hello");

                    handle.spawn(
                        channel.basic_consume("hello", "my_consumer", &BasicConsumeOptions::default(), &FieldTable::new()).and_then(|stream| {
                            println!("got consumer stream");

                            stream.for_each(move |message| {
                                println!("got message: {:?}", message);
                                println!("decoded message: {:?}", std::str::from_utf8(&message.data).unwrap());
                                ack_channel.basic_ack(message.delivery_tag)
                            })
                        }).map_err(|err| {
                            println!("uh oh: {:?}", err);
                            ()
                        })
                    );
                    futures::future::ok::<(), std::io::Error>(())
                }).and_then(move |_| {
                    client.create_channel().and_then(|publish_channel| {
                        let id = publish_channel.id;
                        println!("created channel with id: {}", id);

                        publish_channel.exchange_declare("hello_exchange", "direct", &ExchangeDeclareOptions::default(), &FieldTable::new()).and_then(move |_| {
                            publish_channel.queue_bind("hello", "hello_exchange", "hello_2", &QueueBindOptions::default(), &FieldTable::new()).and_then(move |_| {
                                publish_channel.basic_publish(
                                    "hello_exchange",
                                    "hello_2",
                                    b"hello from tokio",
                                    &BasicPublishOptions::default(),
                                    BasicProperties::default().with_user_id("guest".to_string()).with_reply_to("foobar".to_string())
                                ).map(|confirmation| {
                                    println!("publish got confirmation: {:?}", confirmation)
                                }).and_then(move |_| {
                                    publish_channel.exchange_bind("hello_exchange", "amq.direct", "test_bind", &ExchangeBindOptions::default(), &FieldTable::new()).and_then(move |_| {
                                            publish_channel.exchange_unbind("hello_exchange", "amq.direct", "test_bind", &ExchangeUnbindOptions::default(), &FieldTable::new()).and_then(move |_| {
                                                    publish_channel.exchange_delete("hello_exchange", &ExchangeDeleteOptions::default()).and_then(move |_| {
                                                            publish_channel.close(200, "Bye")
                                                    })
                                            })
                                    })
                                })
                            })
                        })
                    })
                })
            })
        })
    ).unwrap();

    core.run(futures::future::empty::<(), ()>()).unwrap();
}
