// =============================================================
// Author : Aaditya Malla (CSCI 312 - Group 4)
// File   : rocket/src/main.rs
// Purpose: Rust (Rocket) back end. Validates every request coming
//          from the client (through NGINX) and forwards it to the
//          Spark container, which queries the Delta Lake table in S3.
// =============================================================
#[macro_use]
extern crate rocket;

use rocket::http::Status;
use rocket::serde::json::{json, Json, Value};
use rocket::serde::Deserialize;

type ApiResult = Result<Json<Value>, (Status, Json<Value>)>;

fn spark_url() -> String {
    std::env::var("SPARK_URL").unwrap_or_else(|_| "http://spark:9000".into())
}

fn container_name() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into())
}

fn bad_request(msg: &str) -> (Status, Json<Value>) {
    (Status::BadRequest, Json(json!({ "error": msg })))
}

fn spark_unreachable(_: reqwest::Error) -> (Status, Json<Value>) {
    (Status::BadGateway, Json(json!({ "error": "Spark / Delta Lake service unreachable" })))
}

/// Letters and spaces only, 1-40 characters (e.g. "Computer Science").
fn valid_text(s: &str) -> bool {
    !s.is_empty() && s.len() <= 40 && s.chars().all(|c| c.is_ascii_alphabetic() || c == ' ')
}

/// Wrap Spark's reply so the client can see which containers handled it.
async fn wrap(resp: reqwest::Response) -> ApiResult {
    let code = resp.status().as_u16();
    let body: Value = resp.json().await.map_err(spark_unreachable)?;
    let out = json!({
        "served_by": container_name(),
        "path": "client -> nginx -> rocket -> spark -> delta lake (s3)",
        "result": body
    });
    if code >= 400 {
        Err((Status::from_code(code).unwrap_or(Status::BadGateway), Json(out)))
    } else {
        Ok(Json(out))
    }
}

#[get("/")]
fn index() -> String {
    format!("Hello from Rocket (Rust)! Container: {}\n", container_name())
}

/// Client query: /students?major=...&min_gpa=...&version=...
#[get("/students?<major>&<min_gpa>&<version>")]
async fn students(major: Option<String>, min_gpa: Option<String>, version: Option<String>) -> ApiResult {
    let mut params: Vec<(&str, String)> = Vec::new();

    if let Some(m) = major.as_deref().map(str::trim).filter(|m| !m.is_empty()) {
        if !valid_text(m) {
            return Err(bad_request("major must be letters and spaces only (max 40)"));
        }
        params.push(("major", m.to_string()));
    }
    if let Some(g) = min_gpa.as_deref().map(str::trim).filter(|g| !g.is_empty()) {
        match g.parse::<f64>() {
            Ok(v) if (0.0..=4.0).contains(&v) => params.push(("min_gpa", v.to_string())),
            _ => return Err(bad_request("min_gpa must be a number from 0 to 4")),
        }
    }
    if let Some(v) = version.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
        match v.parse::<u32>() {
            Ok(n) => params.push(("version", n.to_string())),
            _ => return Err(bad_request("version must be a whole number")),
        }
    }

    let resp = reqwest::Client::new()
        .get(format!("{}/students", spark_url()))
        .query(&params)
        .send()
        .await
        .map_err(spark_unreachable)?;
    wrap(resp).await
}

#[derive(Deserialize)]
#[serde(crate = "rocket::serde")]
struct NewStudent {
    name: String,
    major: String,
    gpa: f64,
    year: u8,
}

/// Client write: POST /students  {"name","major","gpa","year"}
#[post("/students", format = "json", data = "<s>")]
async fn add_student(s: Json<NewStudent>) -> ApiResult {
    let name = s.name.trim();
    let major = s.major.trim();
    if !valid_text(name) || !valid_text(major) {
        return Err(bad_request("name and major must be letters and spaces only (max 40)"));
    }
    if !(0.0..=4.0).contains(&s.gpa) {
        return Err(bad_request("gpa must be from 0 to 4"));
    }
    if !(1..=4).contains(&s.year) {
        return Err(bad_request("year must be 1 to 4"));
    }

    let resp = reqwest::Client::new()
        .post(format!("{}/students", spark_url()))
        .json(&json!({ "name": name, "major": major, "gpa": s.gpa, "year": s.year }))
        .send()
        .await
        .map_err(spark_unreachable)?;
    wrap(resp).await
}

/// Delta Lake table history (one entry per version)
#[get("/history")]
async fn history() -> ApiResult {
    let resp = reqwest::get(format!("{}/history", spark_url()))
        .await
        .map_err(spark_unreachable)?;
    wrap(resp).await
}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![index, students, add_student, history])
}
