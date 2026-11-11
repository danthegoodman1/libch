//! `libch-demo`: runs the recipe against the dev stack and prints its report.

use std::process::ExitCode;
use std::time::Duration;

use libch_demo::{Config, run};
use libch_testkit::stack;

const USAGE: &str = "libch-demo [--tenants N] [--batches N] [--rows-per-batch N] [--max-batch-rows N] [--batch-age-ms N] [--events-per-second N] \
[--zipf EXPONENT] [--operations N] [--waiting-workers N] [--setups N] \
[--compactions N] [--part-threshold N] [--grace SECONDS] [--queried-tenants N] [--fleet-servers N] [--checked-tenants N] [--stock-sample N] [--kill-one-in N] [--seed N]";

fn parse() -> Result<Config, String> {
    let mut config = Config::default();
    let mut arguments = std::env::args().skip(1);
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or_else(|| format!("{flag} takes a value"))?;
        let number = || value.parse::<usize>().map_err(|_| format!("{flag} takes a number, and `{value}` is none"));
        match flag.as_str() {
            "--tenants" => config.tenants = number()?,
            "--batches" => config.batches = number()?,
            "--rows-per-batch" => config.rows_per_batch = number()?,
            "--max-batch-rows" => config.max_batch_rows = number()?,
            "--batch-age-ms" => config.max_batch_age = Duration::from_millis(number()? as u64),
            "--events-per-second" => config.events_per_second = Some(number()? as u64),
            "--setups" => config.setups_at_once = number()?,
            "--zipf" => config.zipf_exponent = value.parse().map_err(|_| format!("--zipf takes an exponent, and `{value}` is none"))?,
            "--operations" => config.operations_at_once = number()?,
            "--waiting-workers" => config.waiting_workers = number()?,
            "--compactions" => config.compactions_at_once = number()?,
            "--part-threshold" => config.part_threshold = number()?,
            "--grace" => config.grace = Duration::from_secs(number()? as u64),
            "--queried-tenants" => config.queried_tenants = number()?,
            "--fleet-servers" => config.fleet_servers = number()?,
            "--checked-tenants" => config.checked_tenants = number()?,
            "--stock-sample" => config.stock_sample = number()?,
            "--kill-one-in" => config.kill_one_in = Some(number()? as u32),
            "--seed" => config.seed = number()? as u64,
            _ => return Err(format!("unknown flag {flag}\n{USAGE}")),
        }
    }
    if config.tenants == 0 || config.operations_at_once == 0 || config.compactions_at_once == 0 {
        return Err("a run needs at least one tenant, one operation at a time and one compaction at a time".into());
    }
    Ok(config)
}

#[tokio::main]
async fn main() -> ExitCode {
    let config = match parse() {
        Ok(config) => Config { progress: true, ..config },
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    match run(stack().await, &config).await {
        Ok(report) => {
            print!("{report}");
            if report.is_exact() { ExitCode::SUCCESS } else { ExitCode::FAILURE }
        }
        Err(error) => {
            eprintln!("the run failed: {error}");
            ExitCode::FAILURE
        }
    }
}
