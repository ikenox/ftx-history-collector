use std::path::PathBuf;
use std::process::exit;
use std::time::SystemTime;

use anyhow::{Context, Error, Result};
use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, Utc};
use clap::Parser;
use csv_async::AsyncSerializer;
use futures::FutureExt;
use futures::StreamExt;
use log::*;
use serde::{Deserialize, Serialize};
use surf::Request;
use tokio::fs::File;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    // A json file path of your FTX credential.
    #[clap(long, parse(from_os_str))]
    credential: PathBuf,
    // An output directory.
    #[clap(long, parse(from_os_str))]
    outdir: PathBuf,
    // optional. If not specified, the script will download main account's data.
    #[clap(long)]
    sub_account: Option<String>,
    // optional. inclusive yyyy-MM-dd starting date.
    #[clap(long)]
    start: Option<NaiveDate>,
    // optional. exclusive yyyy-MM-dd ending date.
    #[clap(long)]
    end: Option<NaiveDate>,
}

#[tokio::main]
async fn main() {
    std::env::set_var(
        "RUST_LOG",
        std::env::var("RUST_LOG").unwrap_or("info,surf=warn".to_string()),
    );
    env_logger::init();

    let args: Args = Args::parse();
    if args
        .start
        .zip(args.end)
        .map(|(start, end)| start >= end)
        .unwrap_or(false)
    {
        error!("end date must be greater than start date");
        exit(1);
    }

    let cred: FtxCredential = serde_json::from_str(
        &tokio::fs::read_to_string(args.credential)
            .await
            .expect("failed to read credential file"),
    )
    .expect("failed to parse credential file");

    let outdir = &args.outdir;
    let sub_account = &args.sub_account;
    let start_time = args.start.map(|d| d.and_hms(0, 0, 0));
    let end_time = args
        .end
        .map(|d| d.and_hms(0, 0, 0))
        .unwrap_or(Utc::now().naive_utc().date().and_hms(0, 0, 0));

    futures::stream::unfold(
        RequestCursor {
            end_time: end_time.clone(),
            oldest_fill_id: u64::MAX,
        },
        |RequestCursor {
             end_time,
             oldest_fill_id,
         }| {
            // FTX API returns up to 5000 fills order by time desc
            // So always specifying start_time=zero and moves end_time to obtain all fills
            get_fills(0, end_time.timestamp(), &cred, sub_account).map(move |result| {
                let fills = result
                    .expect("failed to request")
                    .into_iter()
                    .filter(|f: &FtxFill| {
                        // avoid duplication
                        f.id < oldest_fill_id
                            // newer than the specified start time
                            && start_time
                                .map(|st| st <= f.time.naive_utc())
                                .unwrap_or(true)
                    })
                    .collect::<Vec<_>>();
                let next_cursor = fills.last().map(|oldest: &FtxFill| {
                    info!(
                        "{} fills between {} and {} ({} - {})",
                        fills.len(),
                        oldest.time.timestamp(),
                        end_time.timestamp(),
                        // TODO use the specified timezone
                        oldest.time.naive_utc().format("%Y-%m-%dT%H:%M:%S"),
                        end_time.format("%Y-%m-%dT%H:%M:%S"),
                    );
                    RequestCursor {
                        // +1 second because some fills on the same second maybe still remaining
                        end_time: oldest.time.naive_utc() + chrono::Duration::seconds(1),
                        oldest_fill_id: oldest.id,
                    }
                });

                next_cursor.map(|c| (fills, c))
            })
        },
    )
    .flat_map(|fills| futures::stream::iter(fills))
    .fold(None, |cursor, fill: FtxFill| async move {
        let fill_date = fill.time.date().naive_utc();
        let mut writer = cursor
            .and_then(
                |WriterCursor {
                     target_date: date,
                     writer,
                 }| {
                    if date == fill_date {
                        // continue writing to current file
                        Some(futures::future::ready(writer).left_future())
                    } else {
                        // date is changed so change file to write
                        None
                    }
                },
            )
            .unwrap_or_else(|| {
                // date is changed or cursor is not initialized yet
                new_writer(outdir, sub_account, &fill_date)
                    .map(|result| result.expect("failed to open a new file"))
                    .right_future()
            })
            .await;
        writer
            .serialize(&fill)
            .await
            .expect("failed to write data to file");
        Some(WriterCursor {
            target_date: fill_date,
            writer,
        })
    })
    .await;
}

async fn get_fills(
    // inclusive
    start_time: i64,
    // exclusive
    end_time: i64,
    credential: &FtxCredential,
    sub_account: &Option<String>,
) -> Result<Vec<FtxFill>> {
    let response_body = surf::client()
        .send(signed_request(
            surf::get(format!(
                "https://ftx.com/api/fills?start_time={}&end_time={}",
                start_time, end_time
            ))
            .build(),
            credential,
            sub_account,
        ))
        .await
        .map_err(Error::msg)?
        .body_string()
        .await
        .map_err(Error::msg)?;

    serde_json::from_str::<FtxResponse<_>>(&response_body)
        .with_context(|| {
            format!(
                "unexpected response json format. \n\nresponse body:\n{}",
                response_body
            )
        })
        .map(|body_json| body_json.result)
}

fn signed_request(
    mut rb: Request,
    credential: &FtxCredential,
    sub_account: &Option<String>,
) -> Request {
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let text = format!(
        "{}{}{}{}",
        ts,
        rb.method().to_string(),
        rb.url().path(),
        rb.url()
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_else(|| "".to_string())
    );
    rb.set_header("FTX-KEY", &credential.api_key);
    rb.set_header("FTX-TS", ts.to_string());
    rb.set_header(
        "FTX-SIGN",
        hex::encode(hmac_sha256::HMAC::mac(&text, &credential.api_secret)),
    );
    if let Some(sub_account) = &sub_account {
        rb.set_header("FTX-SUBACCOUNT", sub_account);
    }

    rb
}

async fn new_writer(
    outdir: &PathBuf,
    sub_account: &Option<String>,
    date: &NaiveDate,
) -> Result<AsyncSerializer<File>> {
    let filepath = outdir.join(format!(
        "{}_{}.csv",
        sub_account
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or_else(|| "main"),
        date,
    ));
    tokio::fs::create_dir_all(outdir)
        .await
        .with_context(|| "failed to create directory to put a file")?;
    let file = File::create(filepath)
        .await
        .with_context(|| "failed to create a file to write")?;
    Ok(csv_async::AsyncSerializer::from_writer(file))
}

struct RequestCursor {
    end_time: NaiveDateTime,
    oldest_fill_id: u64,
}

struct WriterCursor {
    target_date: NaiveDate,
    writer: AsyncSerializer<File>,
}

#[derive(Deserialize)]
struct FtxCredential {
    api_key: String,
    api_secret: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct FtxResponse<T> {
    result: T,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct FtxFill {
    fee: f64,
    fee_currency: Option<String>,
    fee_rate: Option<f64>,
    future: Option<String>,
    id: u64,
    liquidity: Option<String>,
    market: Option<String>,
    base_currency: Option<String>,
    quote_currency: Option<String>,
    order_id: Option<u64>,
    trade_id: Option<u64>,
    price: f64,
    side: Option<String>,
    size: f64,
    time: DateTime<Local>,
    #[serde(rename = "type")]
    typ: Option<String>,
}
