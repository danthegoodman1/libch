//! The few S3 calls libch makes itself: checking that blobs exist, reading a spilled record, and clearing a
//! prefix.

use std::time::Duration;

use bytes::Bytes;
use reqwest::StatusCode;
use rusty_s3::actions::ListObjectsV2;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};

use crate::{Error, S3Location};

const SIGNATURE_LIFETIME: Duration = Duration::from_secs(300);

/// An object in a listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object {
    pub key: String,
    pub size: u64,
}

/// A client for the bucket that holds a tenant's blobs. Keys are whole object keys, prefix included.
#[derive(Clone, Debug)]
pub struct S3Client {
    bucket: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
}

impl S3Client {
    pub fn new(location: &S3Location) -> Result<S3Client, Error> {
        let endpoint = url::Url::parse(&location.endpoint).map_err(|error| Error::S3(format!("endpoint `{}`: {error}", location.endpoint)))?;
        let bucket = Bucket::new(endpoint, UrlStyle::Path, location.bucket.clone(), location.region.clone())
            .map_err(|error| Error::S3(format!("bucket `{}`: {error}", location.bucket)))?;
        Ok(S3Client {
            bucket,
            credentials: Credentials::new(location.access_key.clone(), location.secret_key.clone()),
            http: reqwest::Client::builder().build()?,
        })
    }

    /// The object's size, or `None` when no such object exists.
    pub async fn size(&self, key: &str) -> Result<Option<u64>, Error> {
        let url = self.bucket.head_object(Some(&self.credentials), key).sign(SIGNATURE_LIFETIME);
        let response = self.http.head(url).send().await?;
        match response.status() {
            StatusCode::NOT_FOUND => Ok(None),
            status if status.is_success() => {
                let size = response.headers().get(reqwest::header::CONTENT_LENGTH).and_then(|value| value.to_str().ok()).and_then(|value| value.parse().ok());
                size.map(Some).ok_or_else(|| Error::S3(format!("HEAD {key} answered without a length")))
            }
            status => Err(Error::S3(format!("HEAD {key} answered {status}"))),
        }
    }

    /// The object's bytes, or `None` when no such object exists.
    pub async fn get(&self, key: &str) -> Result<Option<Bytes>, Error> {
        let url = self.bucket.get_object(Some(&self.credentials), key).sign(SIGNATURE_LIFETIME);
        let response = self.http.get(url).send().await?;
        match response.status() {
            StatusCode::NOT_FOUND => Ok(None),
            status if status.is_success() => Ok(Some(response.bytes().await?)),
            status => Err(Error::S3(format!("GET {key} answered {status}"))),
        }
    }

    pub async fn put(&self, key: &str, bytes: Bytes) -> Result<(), Error> {
        let url = self.bucket.put_object(Some(&self.credentials), key).sign(SIGNATURE_LIFETIME);
        // An empty body goes out without a length unless one is set, and S3 requires it.
        let response = self.http.put(url).header(reqwest::header::CONTENT_LENGTH, bytes.len()).body(bytes).send().await?;
        if response.status().is_success() { Ok(()) } else { Err(Error::S3(format!("PUT {key} answered {}", response.status()))) }
    }

    /// Deletes an object. Deleting one that is absent succeeds, as S3 has it.
    pub async fn delete(&self, key: &str) -> Result<(), Error> {
        let url = self.bucket.delete_object(Some(&self.credentials), key).sign(SIGNATURE_LIFETIME);
        let response = self.http.delete(url).send().await?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(Error::S3(format!("DELETE {key} answered {}", response.status())))
        }
    }

    /// Every object whose key starts with `prefix`.
    pub async fn list(&self, prefix: &str) -> Result<Vec<Object>, Error> {
        let mut objects = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut action = self.bucket.list_objects_v2(Some(&self.credentials));
            action.with_prefix(prefix);
            if let Some(token) = &continuation {
                action.with_continuation_token(token.as_str());
            }
            let response = self.http.get(action.sign(SIGNATURE_LIFETIME)).send().await?;
            if !response.status().is_success() {
                return Err(Error::S3(format!("listing `{prefix}` answered {}", response.status())));
            }
            let text = response.text().await?;
            let page = ListObjectsV2::parse_response(&text).map_err(|error| Error::S3(format!("listing `{prefix}`: {error}")))?;
            objects.extend(page.contents.into_iter().map(|object| Object { key: object.key, size: object.size }));
            match page.next_continuation_token {
                Some(token) => continuation = Some(token),
                None => return Ok(objects),
            }
        }
    }

    /// Deletes every object whose key starts with `prefix`, and returns how many there were.
    pub async fn delete_prefix(&self, prefix: &str) -> Result<usize, Error> {
        let objects = self.list(prefix).await?;
        for object in &objects {
            self.delete(&object.key).await?;
        }
        Ok(objects.len())
    }
}
