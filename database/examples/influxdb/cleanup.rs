//! Explicit table deletion; keyed deletes are refused by name.
use super::{Result, model::Reading};
use database::store::{DB, Error, Model};

pub async fn run(db: &DB) -> Result<()> {
    match db.delete(Reading::resource(), "boiler").await {
        Err(Error::Unimplemented { capability, .. }) => println!("   InfluxDB cannot {capability}"),
        other => return Err(format!("expected a refusal, got {other:?}").into()),
    }
    // This removes schema AND every point, only in this run's disposable DB.
    db.schema.drop_schema(Reading::resource()).await?;
    println!("   Deleted the example table and all its observations");
    Ok(())
}
