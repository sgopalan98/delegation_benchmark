mod tcp_helper;
mod workloads;

use std::{thread, net::TcpStream, sync::{Barrier, Arc}, fs::OpenOptions, time::Duration};

use csv::WriterBuilder;
use rand::{seq::SliceRandom, RngCore};
use structopt::StructOpt;
use workloads::*;


#[derive(Debug, StructOpt, Clone)]
pub struct Opt {
    #[structopt(short = "k", long = "experiment", default_value = "ReadHeavy")]
    pub experiment: workloads::WorkloadKind,
    #[structopt(short = "d", long = "delegation", default_value = "1")]
    pub delegation_no: u8,
    #[structopt(short = "c", default_value = "15")]
    pub capacity: u8,
    #[structopt(short = "t", long, default_value = "1")]
    pub threads: usize,
    #[structopt(short = "o", long, default_value = "1")]
    pub ops_st: usize
}


fn mix_multiple(
    socket: TcpStream, 
    keys: &[u64],
    op_mix: &[Operation],
    ops: usize,
    ops_st : usize,
    prefilled: usize,
    barrier: Arc<Barrier>,
) {
    // Invariant: erase_seq <= insert_seq
    // Invariant: insert_seq < numkeys
    let nkeys = keys.len();
    let mut erase_seq = 0;
    let mut insert_seq = prefilled;
    let mut find_seq = 0;

    // We're going to use a very simple LCG to pick random keys.
    // We want it to be _super_ fast so it doesn't add any overhead.
    assert!(nkeys.is_power_of_two());
    assert!(nkeys > 4);
    assert_eq!(op_mix.len(), 100);
    let a = nkeys / 2 + 1;
    let c = nkeys / 4 - 1;
    let find_seq_mask = nkeys - 1;

    // The elapsed time is measured by the lifetime of `workload_scope`.
    let workload_scope = scopeguard::guard(barrier, |barrier| {
        barrier.wait();
    });
    workload_scope.wait();

    let mut operations = Vec::new();
    let mut mul_keys = Vec::new();
    let mut assertions = Vec::new();

    for (i, op) in (0..(ops / op_mix.len()))
        .flat_map(|_| op_mix.iter())
        .enumerate()
    {
        if i == ops {
            break;
        }

        match op {
            Operation::Read => {
                let should_find = find_seq >= erase_seq && find_seq < insert_seq;
                if find_seq >= erase_seq {
                    operations.push(1);
                    mul_keys.push(keys[find_seq]);
                    assertions.push(should_find);
                } else {
                    // due to upserts, we may _or may not_ find the key
                }

                // Twist the LCG since we used find_seq
                find_seq = (a * find_seq + c) & find_seq_mask;
            }

            Operation::Insert => {
                operations.push(2);
                mul_keys.push(keys[insert_seq]);
                assertions.push(true);

                insert_seq += 1;
            }

            Operation::Remove => {
                if erase_seq == insert_seq {
                    // If `erase_seq` == `insert_eq`, the table should be empty.
                    // let removed = tbl.remove(&keys[find_seq]);
                    operations.push(3);
                    mul_keys.push(keys[find_seq]);
                    assertions.push(false);

                    // Twist the LCG since we used find_seq
                    find_seq = (a * find_seq + c) & find_seq_mask;
                } else {
                    operations.push(3);
                    mul_keys.push(keys[erase_seq]);
                    assertions.push(true);
                    erase_seq += 1;
                }
            }

            Operation::Update => {
                // Same as find, except we update to the same default value
                let should_exist = find_seq >= erase_seq && find_seq < insert_seq;
                if find_seq >= erase_seq {
                    operations.push(4);
                    mul_keys.push(keys[find_seq]);
                    assertions.push(should_exist);
                } else {
                    // due to upserts, we may or may not have updated an existing key
                }

                // Twist the LCG since we used find_seq
                find_seq = (a * find_seq + c) & find_seq_mask;
            }

            Operation::Upsert => {}
        }
        // If 100 operations are complete, execute.
        if operations.len() ==  ops_st {
            let results = tcp_helper::execute(socket.try_clone().unwrap(), ops_st, operations.clone(), mul_keys);


            for index in 0..assertions.len() {
                assert_eq!(results[index], assertions[index], "Something failed");
            }
            operations = Vec::new();
            mul_keys = Vec::new();
            assertions = Vec::new();
        }
    }

    if operations.len() != 0 {
        let results = tcp_helper::execute(socket.try_clone().unwrap(), ops_st, operations, mul_keys);
        for index in 0..assertions.len() {
            assert_eq!(results[index], assertions[index], "Something failed");
        }
    }

    let close_operation = Vec::new();
    let close_key = Vec::new();
    tcp_helper::execute(socket.try_clone().unwrap(), ops_st, close_operation, close_key);
}

fn bench(options: Opt, threads: usize) -> Duration {
    let seed = rand::random();
    let mut rng: rand::rngs::SmallRng = rand::SeedableRng::from_seed(seed);

    // parameters of the experiment
    let capacity_log_2 = options.capacity;
    let initial_capacity = 1 << capacity_log_2;
    let ops_f = 1;
    let total_ops = (initial_capacity * ops_f) as usize;
    let ops_st = options.ops_st;

    // Connect to server
    tcp_helper::connect_server(options.delegation_no, initial_capacity, threads, ops_st);

    // Get sockets for each thread
    let mut socket_threads = vec![];
    let mut sockets = vec![];

    for _ in 0..threads {
        socket_threads.push(thread::spawn(move || {
            tcp_helper::get_client_socket()
        }));
    }

    sockets =  socket_threads
    .into_iter()
    .map(|jh| jh.join().unwrap())
    .collect();


    // Create workload
    let experiment = workloads::create(&options, threads as u32);
    let prefill_f = experiment.prefill_f;
    let read = experiment.mix.read;
    let insert = experiment.mix.insert;
    let remove = experiment.mix.remove;
    let update = experiment.mix.update;
    let upsert = experiment.mix.upsert;

    // Generating operations mix
    println!("generating operation mix");
    let mut op_mix = Vec::with_capacity(100);
    op_mix.append(&mut vec![Operation::Read; usize::from(read as u8)]);
    op_mix.append(&mut vec![Operation::Insert; usize::from(insert as u8)]);
    op_mix.append(&mut vec![Operation::Remove; usize::from(remove as u8)]);
    op_mix.append(&mut vec![Operation::Update; usize::from(update as u8)]);
    op_mix.append(&mut vec![Operation::Upsert; usize::from(upsert as u8)]);
    op_mix.shuffle(&mut rng);
    println!("generated operation mix");
    println!();

    println!("generating key space");
    let prefill = (initial_capacity as f64 * prefill_f) as usize;
    // We won't be running through `op_mix` more than ceil(total_ops / 100), so calculate that
    // ceiling and multiply by the number of inserts and upserts to get an upper bound on how
    // many elements we'll be inserting.
    let max_insert_ops =
        (total_ops + 99) / 100 * usize::from(insert + upsert as u8);
    let insert_keys = std::cmp::max(initial_capacity, max_insert_ops) + prefill;
    // Round this quantity up to a power of 2, so that we can use an LCG to cycle over the
    // array "randomly".
    let insert_keys_per_thread =
        ((insert_keys + threads - 1) / threads).next_power_of_two();

    // Generate keys for each threads
    let mut generators = Vec::new();
    for _ in 0..threads {
        let mut thread_seed = [0u8; 32];
        rng.fill_bytes(&mut thread_seed[..]);
        generators.push(std::thread::spawn(move || {
            let mut rng: rand::rngs::SmallRng = rand::SeedableRng::from_seed(thread_seed);
            let mut keys= Vec::with_capacity(insert_keys_per_thread);
            keys.extend((0..insert_keys_per_thread).map(|_| rng.next_u64()));
            keys
        }));
    }
    let keys: Vec<_> = generators
        .into_iter()
        .map(|jh| jh.join().unwrap())
        .collect();


    // And pre-fill it
    let prefill_per_thread = prefill / threads;
    let mut prefillers = Vec::new();
    

    let mut thread_index = 0;
    println!("starting prefilling");
    for  keys in keys{
        let mut sockets_clone = Vec::new();
        for socket in &sockets {
            sockets_clone.push(socket.try_clone().unwrap());
        }
        let socket = sockets_clone[thread_index].try_clone().unwrap();
        prefillers.push(std::thread::spawn(move || {
            let mut start_index = 0;

            loop {
                if start_index >= prefill_per_thread {
                    break;
                }

                let max_index = std::cmp::min(start_index + ops_st, prefill_per_thread);

                let mut operations = Vec::new();
                let mut mul_keys = Vec::new();

                for index in start_index..max_index {
                    operations.push(2);
                    mul_keys.push(keys[index]);
                }

                let results = tcp_helper::execute(socket.try_clone().unwrap(), ops_st, operations, mul_keys);
                for result in results {
                    assert!(result);
                }

                start_index = start_index + ops_st;
            }

            let close_operation = Vec::new();
            let close_key = Vec::new();
            let results = tcp_helper::execute(socket.try_clone().unwrap(), ops_st, close_operation, close_key);
            keys
        }
        ));
        thread_index = thread_index + 1;
    }
    let keys: Vec<_> = prefillers
        .into_iter()
        .map(|jh| jh.join().unwrap())
        .collect();
    println!("done prefilling");



    let mut socket_threads = vec![];
    let mut sockets = vec![];
    println!("Creating socket threads");
    for thread_index in 0..threads {
        socket_threads.push(thread::spawn(move || {
            println!("in new thread {}", thread_index);
            tcp_helper::get_client_socket()
        }));
    }
    sockets =  socket_threads
    .into_iter()
    .map(|jh| jh.join().unwrap())
    .collect();

    println!("start workload mix");
    let ops_per_thread = total_ops / threads;
    let op_mix = Arc::new(op_mix.into_boxed_slice());
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut mix_threads = Vec::with_capacity(threads);
    let mut thread_index = 0;
    for keys in keys {
        let op_mix = Arc::clone(&op_mix);
        let barrier = Arc::clone(&barrier);
        let mut sockets_clone = Vec::new();
        for socket in &sockets {
            sockets_clone.push(socket.try_clone().unwrap());
        }
        let socket = sockets_clone[thread_index].try_clone().unwrap();
        mix_threads.push(std::thread::spawn(move || {
            mix_multiple(
                socket,
                &keys,
                &op_mix,
                ops_per_thread,
                ops_st,
                prefill_per_thread,
                barrier,
            );
        }));
        thread_index += 1;
    }

    barrier.wait();
    let start = std::time::Instant::now();
    barrier.wait();


    let _samples: Vec<_> = mix_threads
        .into_iter()
        .map(|jh| jh.join().unwrap())
        .collect();
    println!("workload mix done");
    let spent = start.elapsed();

    return spent;
}

fn main() {
    // First get the information for the experiment

    let options = Opt::from_args();
    let threads = options.threads;

    // Open the CSV file with write mode and append flag
    let file = OpenOptions::new()
                                .write(true)
                                .append(true)
                                .open(format!("results_{}/{:?}.csv", options.delegation_no, options.experiment)).unwrap();
    // TODO: Repeat the experiment multiple times and aggregate
    // TODO: Don't pass the whole options - not actually required
    // TODO: Compute latency too
    let elapsed = bench(options.clone(), threads);
    println!("The elapsed is {:?}", elapsed.as_secs_f64());
    // Write to file
    // Create a CSV writer
    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .from_writer(file);

    let capacity_log_2 = options.capacity;
    // Compute throughput
    let capacity = 1 << capacity_log_2;
    let throughput = capacity as f64 / (elapsed.as_secs_f64() * 10f64.powi(6)) ;

    // Write to csv
    writer.write_record(&["DelegationServer".to_string(), capacity.to_string(), threads.to_string(), elapsed.as_secs_f64().to_string(), throughput.to_string(), 0.to_string()]).unwrap();
    writer.flush().unwrap();
}
