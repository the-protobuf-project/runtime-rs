//! Minimal usage example: building a filter predicate and a masked update patch, the two
//! building blocks generated resource clients compose into `List`/`Update` requests.

use network_graphql::{and, ColumnPatch, Nullable, Predicate, StringField};

struct UpdateOrganisationInput {
    display_name: Nullable<String>,
}

impl ColumnPatch for UpdateOrganisationInput {
    fn set_columns(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut out = serde_json::Map::new();
        if let Some(v) = self.display_name.to_set_entry() {
            out.insert("displayName".into(), v);
        }
        out
    }
}

fn main() {
    let name = StringField { col: "displayName" };
    let email = StringField { col: "email" };

    let filter: Predicate = and(&[name.ilike("%rick%"), email.is_null(false)]);
    println!("filter: {}", serde_json::to_string(&filter).unwrap());

    let patch = UpdateOrganisationInput { display_name: Nullable::value("Rick Sanchez".to_string()) };
    assert!(patch.display_name.is_set());
    println!("update columns: {:?}", patch.set_columns());
}
