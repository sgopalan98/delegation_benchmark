use std::{net::{TcpStream, SocketAddr}, io::{BufReader, Write, BufRead, Read, self}, time::Duration};



fn read_command(stream: &mut TcpStream) -> String {
    let mut input = String::new();
    let mut reader = BufReader::new(stream);
    reader.read_line(&mut input).unwrap();
    let input: String = input.trim().to_owned();
    return input;
}

fn write_string(stream: &mut TcpStream, output: String) {
    stream.write(output.as_bytes()).unwrap();
}


pub(crate) fn connect_server(delegation_no: u8, capacity: usize, no_of_threads: usize, ops_st: usize) {
    let mut stream = TcpStream::connect("0.0.0.0:7879").unwrap();
    let command = format!("{} {} {} {}\n", delegation_no, capacity, no_of_threads, ops_st);
    write_string(&mut stream, command);
}

pub(crate) fn get_client_socket() -> TcpStream {
    loop {
        println!("attempting to connect");
        // Create a socket address for the server you want to connect to
        let server_address: SocketAddr = "0.0.0.0:7879".parse().unwrap();

        // Set the desired connection timeout duration
        let timeout_duration = Duration::from_secs(5);

        let result = TcpStream::connect_timeout(&server_address, timeout_duration);
        println!("Got result");
        if result.is_ok() {
            println!("Connected");
            return result.unwrap();
        }
        else {
            println!("Not able to connect");
        }
    }
}

pub(crate) fn execute(mut stream: TcpStream, ops_st:usize, operations: Vec<u8>, keys: Vec<u64>) -> Vec<bool> {
    let mut command = vec![0u8; 9 * ops_st];
    for index in 0..operations.len() {
        let start_index = 9 * index;
        let end_index = 9 * index + 9;
        command[9 * index] = operations[index];
        command.splice((start_index + 1)..end_index, keys[index].to_be_bytes());
    }
    stream.write(&command);
    let mut buf = vec![0u8; ops_st];
    let _result = stream.read_exact(&mut buf);
    let mut results = vec![];
    for error_code in buf {
        results.push(error_code == 0);
    }
    return results;
}