//! Reading an IPC stream from the middle.
//!
//! The stream format has no way in but the front, yet a batch's bytes do not depend on those
//! before it: the columns here hold no dictionaries, so the schema message alone is enough
//! context. Replaying that message and then seeking gives a reader at any batch boundary.

use super::errors::{backend, invalid};
use super::schema::from_schema;
use crate::store::{Resource, Result};
use arrow_array::{Array, RecordBatch, TimestampNanosecondArray};
use arrow_ipc::reader::StreamReader;
use arrow_schema::ArrowError;
use std::fs::File;
use std::io::{BufReader, Cursor, ErrorKind, Read, Seek, SeekFrom};
use std::path::Path;

/// A reader that knows how many bytes have been taken from it. It sits above the buffer, so
/// its position is what the IPC reader consumed rather than what was read ahead.
pub(super) struct Counted<R> {
    inner: R,
    pub(super) position: u64,
}

impl<R: Read> Read for Counted<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.position += read as u64;
        Ok(read)
    }
}

pub(super) type Resumed = StreamReader<Counted<std::io::Chain<Cursor<Vec<u8>>, BufReader<File>>>>;

/// A stream reader starting at the batch at `offset`, given the stream's schema message.
pub(super) fn resume(
    path: &Path,
    header: &[u8],
    offset: u64,
    projection: Option<Vec<usize>>,
) -> Result<Resumed> {
    let mut file = File::open(path).map_err(backend)?;
    file.seek(SeekFrom::Start(offset)).map_err(backend)?;
    let reader = Counted {
        inner: Cursor::new(header.to_vec()).chain(BufReader::new(file)),
        position: offset - header.len() as u64,
    };
    StreamReader::try_new(reader, projection).map_err(backend)
}

/// Feeds `visit` each complete batch and the offset it ends at, until it returns false, the
/// stream ends, or a truncated tail is met.
pub(super) fn batches(
    mut reader: Resumed,
    mut visit: impl FnMut(RecordBatch, u64) -> Result<bool>,
) -> Result<()> {
    while let Some(batch) = reader.next() {
        match batch {
            Ok(batch) => {
                if !visit(batch, reader.get_ref().position)? {
                    break;
                }
            }
            Err(ArrowError::IoError(_, e)) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(backend(e)),
        }
    }
    Ok(())
}

/// The timestamps of `batch`.
pub(super) fn times<'a>(
    batch: &'a RecordBatch,
    time: &str,
) -> Result<&'a TimestampNanosecondArray> {
    batch
        .column_by_name(time)
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        .ok_or_else(|| invalid("arrow batch has no timestamp column"))
}

/// The file's descriptor and its schema message, whose length is where its batches begin.
pub(super) fn header(path: &Path) -> Result<(Resource, Vec<u8>)> {
    let reader = Counted {
        inner: BufReader::new(File::open(path).map_err(backend)?),
        position: 0,
    };
    let reader = StreamReader::try_new(reader, None).map_err(backend)?;
    let resource = from_schema(&reader.schema())?;
    let mut header = vec![0; reader.get_ref().position as usize];
    File::open(path)
        .and_then(|mut file| file.read_exact(&mut header))
        .map_err(backend)?;
    Ok((resource, header))
}
