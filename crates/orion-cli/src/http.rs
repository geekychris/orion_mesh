//! Tiny `reqwest` wrapper that adds the cluster Bearer token + sensible defaults.

use crate::Ctx;
use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::de::DeserializeOwned;
use serde_json::Value;

fn build(ctx: &Ctx) -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    if let Some(t) = &ctx.token {
        if !t.is_empty() {
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {t}"))
                    .context("invalid token characters")?,
            );
        }
    }
    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .build()?)
}

fn url(ctx: &Ctx, path: &str) -> String {
    format!("{}{}", ctx.controller, path)
}

pub async fn get_json<T: DeserializeOwned>(ctx: &Ctx, path: &str) -> Result<T> {
    let url = url(ctx, path);
    let r = build(ctx)?
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?
        .error_for_status()
        .with_context(|| format!("GET {url}"))?;
    Ok(r.json::<T>().await?)
}

#[allow(dead_code)]
pub async fn get_text(ctx: &Ctx, path: &str) -> Result<String> {
    let url = url(ctx, path);
    let r = build(ctx)?
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?
        .error_for_status()
        .with_context(|| format!("GET {url}"))?;
    Ok(r.text().await?)
}

pub async fn post_yaml(ctx: &Ctx, path: &str, body: String) -> Result<Value> {
    let url = url(ctx, path);
    let r = build(ctx)?
        .post(&url)
        .header(CONTENT_TYPE, "application/yaml")
        .body(body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = r.status();
    let text = r.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("POST {url} -> {status}\n{text}");
    }
    if text.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&text).with_context(|| format!("decoding response from {url}\n{text}"))
}

pub async fn post_empty(ctx: &Ctx, path: &str) -> Result<Value> {
    let url = url(ctx, path);
    let r = build(ctx)?
        .post(&url)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = r.status();
    let text = r.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("POST {url} -> {status}\n{text}");
    }
    if text.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&text).with_context(|| format!("decoding response from {url}\n{text}"))
}

pub async fn delete_path(ctx: &Ctx, path: &str) -> Result<Value> {
    let url = url(ctx, path);
    let r = build(ctx)?
        .delete(&url)
        .send()
        .await
        .with_context(|| format!("DELETE {url}"))?;
    let status = r.status();
    let text = r.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("DELETE {url} -> {status}\n{text}");
    }
    if text.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&text).with_context(|| format!("decoding response from {url}\n{text}"))
}
