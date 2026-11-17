//! Create, update and delete: each reads what it must in Rust, where the descriptor is, then
//! hands every key it touches to one script. Go's `Create`, `Update` and `Delete`.

use std::collections::BTreeSet;

use prost_reflect::DynamicMessage;
use redis::Script;

use super::codec::{Gave, decode, encode, failed, fill_managed};
use super::scripts::{self, DONE, MISSING, MOVED, TAKEN};
use super::unique::unique_values;
use super::{BACKEND, RedisDriver};
use crate::records::{Resource, WriteResult, key_of};
use crate::{Error, Result};

/// How often an update or delete re-reads a record that changed under it before giving up.
const ATTEMPTS: usize = 16;

impl RedisDriver {
    pub(super) async fn create_one(
        &self,
        res: &Resource,
        msg: &DynamicMessage,
    ) -> Result<WriteResult> {
        let msg = fill_managed(res, msg, true, None)?;
        let key = key_of(res, &msg)?;
        if key.is_empty() {
            return Err(Error::Invalid(format!(
                "{BACKEND}: resource {:?} has an empty primary key",
                res.name
            )));
        }
        let mut keys = vec![self.keys.record(res, &key), self.keys.ids(res)];
        keys.extend(self.reservations(res, &msg));
        let code = self
            .run(
                &scripts::CREATE,
                &keys,
                &[&encode(&msg), key.as_bytes()],
                &key,
            )
            .await?;
        match code {
            DONE => Ok(WriteResult::done(msg)),
            MISSING | TAKEN => Err(Error::AlreadyExists),
            other => Err(unexpected(other, &key)),
        }
    }

    pub(super) async fn update_one(
        &self,
        res: &Resource,
        msg: &DynamicMessage,
    ) -> Result<WriteResult> {
        let key = key_of(res, msg)?;
        for _ in 0..ATTEMPTS {
            let Some(before) = self.read(res, &key).await? else {
                return Err(Error::NotFound);
            };
            let old = decode(res, &before)?;
            let next = fill_managed(res, msg, false, Some(&old))?;
            let claims = self.reservations(res, &next);
            let keep: BTreeSet<&String> = claims.iter().collect();
            let stale: Vec<String> = self
                .reservations(res, &old)
                .into_iter()
                .filter(|k| !keep.contains(k))
                .collect();
            let mut keys = vec![self.keys.record(res, &key)];
            keys.extend(claims.iter().cloned());
            keys.extend(stale);
            let count = claims.len().to_string();
            let args: [&[u8]; 4] = [&before, &encode(&next), key.as_bytes(), count.as_bytes()];
            match self.run(&scripts::UPDATE, &keys, &args, &key).await? {
                DONE => return Ok(WriteResult::done(next)),
                MISSING => return Err(Error::NotFound),
                TAKEN => return Err(Error::AlreadyExists),
                MOVED => {}
                other => return Err(unexpected(other, &key)),
            }
        }
        Err(contended("update", &key))
    }

    pub(super) async fn delete_one(&self, res: &Resource, key: &str) -> Result<()> {
        for _ in 0..ATTEMPTS {
            let Some(before) = self.read(res, key).await? else {
                return Err(Error::NotFound);
            };
            let old = decode(res, &before)?;
            let mut keys = vec![self.keys.record(res, key), self.keys.ids(res)];
            keys.extend(self.reservations(res, &old));
            match self
                .run(&scripts::DELETE, &keys, &[&before, key.as_bytes()], key)
                .await?
            {
                DONE => return Ok(()),
                MISSING => return Err(Error::NotFound),
                MOVED => {}
                other => return Err(unexpected(other, key)),
            }
        }
        Err(contended("delete", key))
    }

    /// The reservation key of each unique value `msg` carries.
    fn reservations(&self, res: &Resource, msg: &DynamicMessage) -> Vec<String> {
        unique_values(res, msg)
            .iter()
            .map(|u| self.keys.unique(res, &u.column, &u.value))
            .collect()
    }

    /// Runs `script` over `keys` and `args`, by hash, loading it on a cold server.
    async fn run(
        &self,
        script: &Script,
        keys: &[String],
        args: &[&[u8]],
        key: &str,
    ) -> Result<i64> {
        let mut call = script.prepare_invoke();
        for k in keys {
            call.key(k);
        }
        for a in args {
            call.arg(*a);
        }
        call.invoke_async(&mut self.conn())
            .await
            .map_err(|e| failed(format!("cannot write {key}"), e))
    }
}

/// A record that kept changing between the read and the write, `ATTEMPTS` times running.
fn contended(op: &str, key: &str) -> Error {
    failed(
        format!("cannot {op} {key}"),
        Gave(format!("it changed under {ATTEMPTS} attempts in a row")),
    )
}

/// A script reply none of the scripts sends.
fn unexpected(code: i64, key: &str) -> Error {
    failed(
        format!("cannot write {key}"),
        Gave(format!("the script replied {code}")),
    )
}
