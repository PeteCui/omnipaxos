use core::time;
use std::{thread};

use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
use tokio::net::{TcpStream, TcpListener};

mod messages;
use crate::messages::*;

const CLIENT: &str = "127.0.0.1:8000";

//When running this test
//make sure you have boot the kv-store correctly
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{

    //lisent for incoming result from kv store
    let listener = TcpListener::bind(CLIENT).await.unwrap();

    //test1 read a Non-existent key-value from node 1
    //create message
    let message = CMDMessage{
        operation: Operaiton::Read,
        kv: KeyValue {
            key: String::from("key"),
            value: 0,
        },
    };
    // //wrap messages
    let wrapped_msg = Package{
        types: Types::CMD,
        msg: Msg::CMD(message),
    };
    //serialization
    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
    //send
    if let Ok(mut tcp_stream) = 
    TcpStream::connect("127.0.0.1:8081").await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(serialized.as_bytes()).await.unwrap();
    }
    let (mut socket, _) = listener.accept().await.unwrap();
    let (r, _) = socket.split();
    let mut reader = BufReader::new(r);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    println!("{}",&buf);
    assert_eq!(&buf, "The value is none");
    buf.clear();
    

    //test2 write key:1 to node 1
    // //create message
    let message = CMDMessage{
        operation: Operaiton::Write,
        kv: KeyValue {
            key: String::from("key"),
            value: 0,
        },
    };
    // //wrap messages
    let wrapped_msg = Package{
        types: Types::CMD,
        msg: Msg::CMD(message),
    };
    //serialization
    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
    //send
    if let Ok(mut tcp_stream) = 
    TcpStream::connect("127.0.0.1:8081").await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(serialized.as_bytes()).await.unwrap();
    }
    let (mut socket, _) = listener.accept().await.unwrap();
    let (r, _) = socket.split();
    let mut reader = BufReader::new(r);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    println!("{}",&buf);
    assert_eq!(&buf, "Successfully to append log");
    buf.clear();

    thread::sleep(time::Duration::from_millis(10000));

    //test3 read the key-value written from node 1
    let message = CMDMessage{
        operation: Operaiton::Read,
        kv: KeyValue {
            key: String::from("key"),
            value: 0,
        },
    };
    // //wrap messages
    let wrapped_msg = Package{
        types: Types::CMD,
        msg: Msg::CMD(message),
    };
    //serialization
    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
    //send
    if let Ok(mut tcp_stream) = 
    TcpStream::connect("127.0.0.1:8081").await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(serialized.as_bytes()).await.unwrap();
    }
    let (mut socket, _) = listener.accept().await.unwrap();
    let (r, _) = socket.split();
    let mut reader = BufReader::new(r);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    println!("{}",&buf);
    assert_eq!(&buf, "This value is :0");
    buf.clear();

    //test4 read the key-value written from node 2
    let message = CMDMessage{
        operation: Operaiton::Read,
        kv: KeyValue {
            key: String::from("key"),
            value: 0,
        },
    };
    // //wrap messages
    let wrapped_msg = Package{
        types: Types::CMD,
        msg: Msg::CMD(message),
    };
    //serialization
    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
    //send
    if let Ok(mut tcp_stream) = 
    TcpStream::connect("127.0.0.1:8082").await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(serialized.as_bytes()).await.unwrap();
    }
    let (mut socket, _) = listener.accept().await.unwrap();
    let (r, _) = socket.split();
    let mut reader = BufReader::new(r);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    println!("{}",&buf);
    assert_eq!(&buf, "This value is :0");
    buf.clear();


    //test5 update key:2 to node 1
    let message = CMDMessage{
        operation: Operaiton::Write,
        kv: KeyValue {
            key: String::from("key"),
            value: 1,
        },
    };
    // //wrap messages
    let wrapped_msg = Package{
        types: Types::CMD,
        msg: Msg::CMD(message),
    };
    //serialization
    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
    //send
    if let Ok(mut tcp_stream) = 
    TcpStream::connect("127.0.0.1:8081").await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(serialized.as_bytes()).await.unwrap();
    }
    let (mut socket, _) = listener.accept().await.unwrap();
    let (r, _) = socket.split();
    let mut reader = BufReader::new(r);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    println!("{}",&buf);
    assert_eq!(&buf, "Successfully to append log");
    buf.clear();


    //test6 read the updated key-value from node 1
    let message = CMDMessage{
        operation: Operaiton::Read,
        kv: KeyValue {
            key: String::from("key"),
            value: 0,
        },
    };
    // //wrap messages
    let wrapped_msg = Package{
        types: Types::CMD,
        msg: Msg::CMD(message),
    };
    //serialization
    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
    //send
    if let Ok(mut tcp_stream) = 
    TcpStream::connect("127.0.0.1:8081").await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(serialized.as_bytes()).await.unwrap();
    }
    let (mut socket, _) = listener.accept().await.unwrap();
    let (r, _) = socket.split();
    let mut reader = BufReader::new(r);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    println!("{}",&buf);
    assert_eq!(&buf, "This value is :1");
    buf.clear();

    thread::sleep(time::Duration::from_millis(10000));
    //test7 read the updated key-value from node 2
    let message = CMDMessage{
        operation: Operaiton::Read,
        kv: KeyValue {
            key: String::from("key"),
            value: 0,
        },
    };
    // //wrap messages
    let wrapped_msg = Package{
        types: Types::CMD,
        msg: Msg::CMD(message),
    };
    //serialization
    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
    //send
    if let Ok(mut tcp_stream) = 
    TcpStream::connect("127.0.0.1:8082").await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(serialized.as_bytes()).await.unwrap();
    }
    let (mut socket, _) = listener.accept().await.unwrap();
    let (r, _) = socket.split();
    let mut reader = BufReader::new(r);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    println!("{}",&buf);
    assert_eq!(&buf, "This value is :1");
    buf.clear();


    //test8 snapshot
    // //create message
    let message = CMDMessage{
        operation: Operaiton::Snap,
        kv: KeyValue {
            key: String::from("snapshot"),
            value: 0,
        },
    };
    let wrapped_msg = Package{
        types: Types::CMD,
        msg: Msg::CMD(message),
    };
    //serialization
    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
    if let Ok(mut tcp_stream) = 
    TcpStream::connect("127.0.0.1:8081").await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(serialized.as_bytes()).await.unwrap();
    }
    let (mut socket, _) = listener.accept().await.unwrap();
    let (r, _) = socket.split();
    let mut reader = BufReader::new(r);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    println!("{}",&buf);
    //assert_eq!(&buf, "This value is :1");
    buf.clear();
    

    //test9 read after snapshot
    let message = CMDMessage{
        operation: Operaiton::Read,
        kv: KeyValue {
            key: String::from("key"),
            value: 0,
        },
    };
    // //wrap messages
    let wrapped_msg = Package{
        types: Types::CMD,
        msg: Msg::CMD(message),
    };
    //serialization
    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
    //send
    if let Ok(mut tcp_stream) = 
    TcpStream::connect("127.0.0.1:8081").await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(serialized.as_bytes()).await.unwrap();
    }
    let (mut socket, _) = listener.accept().await.unwrap();
    let (r, _) = socket.split();
    let mut reader = BufReader::new(r);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    println!("{}",&buf);
    assert_eq!(&buf, "This value is :1");
    buf.clear();


    //test10 update the key-value after snapshot
    let message = CMDMessage{
        operation: Operaiton::Write,
        kv: KeyValue {
            key: String::from("key"),
            value: 2,
        },
    };
    // //wrap messages
    let wrapped_msg = Package{
        types: Types::CMD,
        msg: Msg::CMD(message),
    };
    //serialization
    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
    //send
    if let Ok(mut tcp_stream) = 
    TcpStream::connect("127.0.0.1:8081").await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(serialized.as_bytes()).await.unwrap();
    }
    let (mut socket, _) = listener.accept().await.unwrap();
    let (r, _) = socket.split();
    let mut reader = BufReader::new(r);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    println!("{}",&buf);
    assert_eq!(&buf, "Successfully to append log");
    buf.clear();


    //test11 read the updated key-value after snapshot
    let message = CMDMessage{
        operation: Operaiton::Read,
        kv: KeyValue {
            key: String::from("key"),
            value: 0,
        },
    };
    // //wrap messages
    let wrapped_msg = Package{
        types: Types::CMD,
        msg: Msg::CMD(message),
    };
    //serialization
    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
    //send
    if let Ok(mut tcp_stream) = 
    TcpStream::connect("127.0.0.1:8081").await{
        let (_, mut w) = tcp_stream.split();
        w.write_all(serialized.as_bytes()).await.unwrap();
    }
    let (mut socket, _) = listener.accept().await.unwrap();
    let (r, _) = socket.split();
    let mut reader = BufReader::new(r);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    println!("{}",&buf);
    assert_eq!(&buf, "This value is :2");
    buf.clear();


    println!("test successfully");
    Ok(())
}
