use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::{Context, Error, Result};
use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, Utc};
use clap::Parser;
use csv_async::AsyncSerializer;
use futures::StreamExt;
use futures::{FutureExt, TryFutureExt};
use log::*;
use serde::{Deserialize, Serialize};
use surf::Request;
use tokio::fs::File;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(long, parse(from_os_str))]
    credential: PathBuf,
    #[clap(long, parse(from_os_str))]
    outdir: PathBuf,
    #[clap(long)]
    sub_account: Option<String>,
}

#[tokio::main]
async fn main() {
    std::env::set_var(
        "RUST_LOG",
        std::env::var("RUST_LOG").unwrap_or("info,surf=warn".to_string()),
    );
    env_logger::init();

    let args: Args = Args::parse();

    let cred: FtxCredential = serde_json::from_str(
        &tokio::fs::read_to_string(args.credential)
            .await
            .expect("failed to read credential file"),
    )
    .expect("failed to parse credential file");

    let outdir = &args.outdir;
    let sub_account = &args.sub_account;
    futures::stream::unfold(
        RequestCursor {
            end_time: Utc::now().naive_utc(),
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
                    // avoid duplication
                    .filter(|f| f.id < oldest_fill_id)
                    .collect::<Vec<_>>();
                let next_cursor = fills.last().map(|oldest| {
                    info!(
                        "{} fills between {} and {}",
                        fills.len(),
                        oldest.time.timestamp(),
                        end_time.timestamp()
                    );
                    RequestCursor {
                        end_time: oldest.time.naive_utc(),
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
                open_new_file(outdir, sub_account, &fill_date)
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
    start_time: i64,
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

async fn open_new_file(
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
pub struct FtxCredential {
    pub api_key: String,
    pub api_secret: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FtxResponse<T> {
    result: T,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FtxFill {
    pub fee: f64,
    pub fee_currency: Option<String>,
    pub fee_rate: Option<f64>,
    pub future: Option<String>,
    pub id: u64,
    pub liquidity: Option<String>,
    pub market: Option<String>,
    pub base_currency: Option<String>,
    pub quote_currency: Option<String>,
    pub order_id: Option<u64>,
    pub trade_id: Option<u64>,
    pub price: f64,
    pub side: Option<String>,
    pub size: f64,
    pub time: DateTime<Local>,
    #[serde(rename = "type")]
    pub typ: Option<String>,
}
