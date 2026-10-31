//! Explicit table deletion; selective row deletion is unsupported on Core.
use super::Result;
use database::{DB, Error, Resource};

pub async fn run(db: &DB, resource: &Resource) -> Result<()> {
    match db.row_deletion() {
        Err(Error::Unsupported { .. }) => {
            println!("   Core does not support selective row deletion")
        }
        Err(error) => return Err(error.into()),
        Ok(_) => println!("   This backend also offers selective row deletion"),
    }
    // This removes schema AND every point, only in this run's disposable DB.
    db.maintenance()?.drop_table(resource).await?;
    println!("   Deleted the example table and all its observations");
    Ok(())
}
