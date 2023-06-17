use std::{fmt::Debug, str::FromStr};

use serde::{Serialize, Deserialize};

use crate::Opt;


#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Operation {
    Read,
    Insert,
    Remove,
    Update,
    Upsert,
}


#[derive(Clone, Debug)]
pub enum WorkloadKind {
    ReadHeavy,
    Exchange,
    RapidGrow,
}

impl FromStr for WorkloadKind {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ReadHeavy" => Ok(Self::ReadHeavy),
            "Exchange" => Ok(Self::Exchange),
            "RapidGrow" => Ok(Self::RapidGrow),
            _ => Err("unknown workload"),
        }
    }
}


#[derive(Clone, Copy, Debug)]
pub struct Workload {
    /// The mix of operations to run.
    pub mix: Mix,

    /// The initial capacity of the table, specified as a power of 2.
    pub initial_cap_log2: u8,

    /// The fraction of the initial table capacity should we populate before running the benchmark.
    pub prefill_f: f64,

    /// Total number of operations as a multiple of the initial capacity.
    pub ops_f: f64,


    /// No of operations to be executed at a stretch
    pub ops_st: usize,

    /// Number of threads to run the benchmark with.
    pub threads: usize,

    /// Random seed to randomize the workload.
    ///
    /// If `None`, the seed is picked randomly.
    /// If `Some`, the workload is deterministic if `threads == 1`.
    seed: Option<[u8; 32]>,
}



impl Workload {
    /// Start building a new benchmark workload.
    pub fn new(threads: usize, mix: Mix) -> Self {
        Self {
            mix,
            initial_cap_log2: 25,
            prefill_f: 0.0,
            ops_f: 0.75,
            ops_st: 1,
            threads,
            seed: None,
        }
    }

    /// Set the initial capacity for the map.
    ///
    /// Note that the capacity will be `2^` the given capacity!
    ///
    /// The number of operations and the number of pre-filled keys are determined based on the
    /// computed initial capacity, so keep that in mind if you change this parameter.
    ///
    /// Defaults to 25 (so `2^25 ~= 34M`).
    pub fn initial_capacity_log2(&mut self, capacity: u8) -> &mut Self {
        self.initial_cap_log2 = capacity;
        self
    }

    /// Set the fraction of the initial table capacity we should populate before running the
    /// benchmark.
    ///
    /// Defaults to 0%.
    pub fn prefill_fraction(&mut self, fraction: f64) -> &mut Self {
        assert!(fraction >= 0.0);
        assert!(fraction <= 1.0);
        self.prefill_f = fraction;
        self
    }


    /// Set the no of operations that need to be performed at a stretch
    ///
    /// Defaults to 1.
    pub fn operations_at_a_stretch(&mut self, ops_st : usize) -> &mut Self {
        self.ops_st = ops_st;
        self
    }

    /// Set the number of operations to run as a multiple of the initial capacity.
    ///
    /// This value can exceed 1.0.
    ///
    /// Defaults to 0.75 (75%).
    pub fn operations(&mut self, multiple: f64) -> &mut Self {
        assert!(multiple >= 0.0);
        self.ops_f = multiple;
        self
    }

    /// Set the seed used to randomize the workload.
    ///
    /// The seed does _not_ dictate thread interleaving, so you will only observe the exact same
    /// workload if you run the benchmark with `nthreads == 1`.
    pub fn seed(&mut self, seed: [u8; 32]) -> &mut Self {
        self.seed = Some(seed);
        self
    }
}


#[derive(Clone, Copy, Debug)]
pub struct Mix {
    /// The percentage of operations in the mix that are reads.
    pub read: u8,
    /// The percentage of operations in the mix that are inserts.
    pub insert: u8,
    /// The percentage of operations in the mix that are removals.
    pub remove: u8,
    /// The percentage of operations in the mix that are updates.
    pub update: u8,
    /// The percentage of operations in the mix that are update-or-inserts.
    pub upsert: u8,
}

fn read_heavy(threads: u32, capacity: u8) -> Workload {
    let mix = Mix {
        read: 98,
        insert: 1,
        remove: 1,
        update: 0,
        upsert: 0,
    };

    *Workload::new(threads as usize, mix)
        .initial_capacity_log2(capacity)
        .prefill_fraction(0.8)
}

fn rapid_grow(threads: u32, capacity: u8) -> Workload {
    let mix = Mix {
        read: 5,
        insert: 80,
        remove: 5,
        update: 10,
        upsert: 0,
    };

    *Workload::new(threads as usize, mix)
        .initial_capacity_log2(capacity)
        .prefill_fraction(0.0)
}

fn exchange(threads: u32, capacity: u8) -> Workload {
    let mix = Mix {
        read: 10,
        insert: 40,
        remove: 40,
        update: 10,
        upsert: 0,
    };

    *Workload::new(threads as usize, mix)
        .initial_capacity_log2(capacity)
        .prefill_fraction(0.8)
}

pub(crate) fn create(options: &Opt, threads: u32) -> Workload {
    let mut workload = match options.experiment {
        WorkloadKind::ReadHeavy => read_heavy(threads, options.capacity),
        WorkloadKind::Exchange => exchange(threads, options.capacity),
        WorkloadKind::RapidGrow => rapid_grow(threads, options.capacity),
    };
    workload
}
