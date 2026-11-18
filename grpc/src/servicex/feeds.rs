use super::*;

/// A feed scans soft-deleted rows as well as live ones. Payload fetches follow
/// every backend page, even when the backend caps the requested page size.
pub struct Feed<M: Send + Sync + 'static> {
    pub repository: Arc<dyn Repository<M>>,
    pub options: Options,
    pub spec: FeedSpec,
}
pub struct FeedSpec {
    pub id: &'static str,
    pub message: &'static str,
    pub parent: &'static str,
    pub read_mask: &'static [&'static str],
    pub lifecycle: &'static [(&'static str, &'static str)],
    pub state_field: &'static str,
    pub states: &'static [(i32, &'static str)],
    pub scope: Option<usize>,
}
#[derive(Default, Clone)]
pub struct FeedQuery {
    pub page_size: i32,
    pub page_token: String,
    pub since: Option<prost_types::Timestamp>,
    pub since_inclusive: bool,
    pub until: Option<prost_types::Timestamp>,
}
#[derive(Clone, Debug)]
pub struct Change {
    pub name: String,
    pub etag: String,
    pub event_type: String,
    pub time: Option<prost_types::Timestamp>,
    pub scope: String,
}
pub(super) fn message<M: Message>(
    o: &Options,
    name: &str,
    value: &M,
) -> Result<DynamicMessage, Status> {
    let desc = o
        .descriptors
        .get_message_by_name(name)
        .ok_or_else(|| Status::internal(format!("missing descriptor {name}")))?;
    DynamicMessage::decode(desc, value.encode_to_vec().as_slice())
        .map_err(|e| Status::internal(e.to_string()))
}
pub(super) fn timestamp(m: &DynamicMessage, name: &str) -> Option<prost_types::Timestamp> {
    if !m.has_field_by_name(name) {
        return None;
    }
    match m.get_field_by_name(name)?.as_ref() {
        Value::Message(v) => v.transcode_to().ok(),
        _ => None,
    }
}
pub(super) fn integer(m: &DynamicMessage, name: &str) -> i32 {
    match m.get_field_by_name(name).as_deref() {
        Some(Value::I32(v)) | Some(Value::EnumNumber(v)) => *v,
        _ => 0,
    }
}
impl<M: Message + Default + Send + Sync + 'static> Feed<M> {
    pub fn change(&self, value: &M) -> Result<Change, Status> {
        let m = message(&self.options, self.spec.message, value)?;
        let create = timestamp(&m, "create_time");
        let update = timestamp(&m, "update_time");
        let delete = timestamp(&m, "delete_time");
        let life = if let Some(d) = delete.as_ref() {
            if update.as_ref().is_some_and(|u| {
                u.seconds > d.seconds + 1 || (u.seconds == d.seconds + 1 && u.nanos > d.nanos)
            }) {
                ""
            } else {
                "deleted"
            }
        } else if create.is_some() && create == update {
            "created"
        } else {
            "updated"
        };
        let event_type = self
            .spec
            .states
            .iter()
            .find(|(n, _)| *n == integer(&m, self.spec.state_field))
            .map(|(_, t)| *t)
            .or_else(|| {
                self.spec
                    .lifecycle
                    .iter()
                    .find(|(s, _)| *s == life)
                    .map(|(_, t)| *t)
            })
            .unwrap_or("");
        let name = text(&m, "name");
        let parts: Vec<_> = name.split('/').collect();
        let scope = self
            .spec
            .scope
            .filter(|i| (i + 1) * 2 <= parts.len())
            .map(|i| parts[..(i + 1) * 2].join("/"))
            .unwrap_or_default();
        Ok(Change {
            name,
            etag: text(&m, "etag"),
            event_type: event_type.into(),
            time: update,
            scope,
        })
    }
    pub async fn read(&self, ctx: &Context, q: FeedQuery) -> Result<(Vec<Change>, String), Status> {
        let mut terms = Vec::new();
        if let Some(s) = q.since {
            terms.push(format!(
                "update_time {} {:?}",
                if q.since_inclusive { ">=" } else { ">" },
                s.to_string()
            ));
        }
        if let Some(u) = q.until {
            terms.push(format!("update_time <= {:?}", u.to_string()));
        }
        let (rows, next) = self
            .repository
            .list(
                ctx,
                self.spec.parent,
                ListInput {
                    page_size: q.page_size,
                    page_token: q.page_token,
                    filter: terms.join(" AND "),
                    order_by: "update_time".into(),
                    show_deleted: true,
                    read_mask: self.spec.read_mask.iter().map(|s| s.to_string()).collect(),
                },
            )
            .await?;
        Ok((
            rows.iter()
                .map(|r| self.change(r))
                .collect::<Result<_, _>>()?,
            next,
        ))
    }
    pub async fn fetch(
        &self,
        ctx: &Context,
        names: &[String],
    ) -> Result<std::collections::BTreeMap<String, (String, Vec<u8>)>, Status> {
        let mut result = std::collections::BTreeMap::new();
        if names.is_empty() {
            return Ok(result);
        }
        let filter = format!(
            "({})",
            names
                .iter()
                .map(|n| format!("name = {}", serde_json::to_string(n).unwrap()))
                .collect::<Vec<_>>()
                .join(" OR ")
        );
        let mut token = String::new();
        let mut seen = std::collections::HashSet::new();
        loop {
            let (rows, next) = self
                .repository
                .list(
                    ctx,
                    self.spec.parent,
                    ListInput {
                        page_size: i32::try_from(names.len()).unwrap_or(i32::MAX),
                        page_token: token,
                        filter: filter.clone(),
                        show_deleted: true,
                        ..Default::default()
                    },
                )
                .await?;
            for r in rows {
                let d = message(&self.options, self.spec.message, &r)?;
                let data = serde_json::to_vec(&d).map_err(|e| Status::internal(e.to_string()))?;
                result.insert(text(&d, "name"), (text(&d, "etag"), data));
            }
            if next.is_empty() {
                return Ok(result);
            }
            if !seen.insert(next.clone()) {
                return Err(Status::internal("feed repeated page token"));
            }
            token = next;
        }
    }
}
