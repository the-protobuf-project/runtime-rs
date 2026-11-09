//! Subjects as OPC UA node ids.

use std::str::FromStr;

use opcua::types::{NodeId, StatusCode};

use crate::{Result, StreamError};

/// A declared subject, parsed: a node id in the server's namespace table, or
/// one naming its namespace by URI, to be resolved against the server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum NodeRef {
    /// `ns=<index>;<type>=<value>`, or a bare `<type>=<value>` in namespace 0.
    Index(NodeId),
    /// `nsu=<uri>;<type>=<value>`: the identifier, held in namespace 0 until
    /// the URI is resolved to the server's index for it.
    Uri(String, NodeId),
}

/// Parses a subject in OPC UA's string form (Part 6, 5.3.1.10): `ns=3;s=Temp`,
/// `i=2258`, `ns=2;g=<guid>`, `ns=2;b=<base64>`, or `nsu=<uri>;s=Temp`.
pub(crate) fn parse(subject: &str) -> Result<NodeRef> {
    let invalid = |_: StatusCode| {
        StreamError::Unsupported(format!(
            "opcua: subject {subject:?} is not an OPC UA node id; a subject is a node id in \
             string form, such as \"ns=3;s=Temperature\" or \"nsu=urn:plant;i=1001\""
        ))
    };
    if let Some(rest) = subject.strip_prefix("nsu=") {
        let (uri, id) = rest
            .split_once(';')
            .ok_or_else(|| invalid(StatusCode::Good))?;
        if uri.is_empty() || id.starts_with("ns=") {
            return Err(invalid(StatusCode::Good));
        }
        // OPC UA escapes ';' and '%' in a namespace URI.
        let uri = uri
            .replace("%3b", ";")
            .replace("%3B", ";")
            .replace("%25", "%");
        return Ok(NodeRef::Uri(uri, NodeId::from_str(id).map_err(invalid)?));
    }
    NodeId::from_str(subject)
        .map(NodeRef::Index)
        .map_err(invalid)
}

#[cfg(test)]
mod tests {
    use opcua::types::Identifier;

    use super::*;

    #[test]
    fn parses_every_form() {
        assert_eq!(
            parse("ns=3;s=Temperature").unwrap(),
            NodeRef::Index(NodeId::new(3, "Temperature"))
        );
        assert_eq!(
            parse("i=2258").unwrap(),
            NodeRef::Index(NodeId::new(0, 2258u32))
        );
        assert_eq!(
            parse("nsu=urn:plant%3bline;i=1001").unwrap(),
            NodeRef::Uri("urn:plant;line".into(), NodeId::new(0, 1001u32))
        );
        match parse("ns=2;g=72962b91-fa75-4ae6-8d28-b404dc7daf63").unwrap() {
            NodeRef::Index(id) => assert!(matches!(id.identifier, Identifier::Guid(_))),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn refuses_what_is_not_a_node_id() {
        for subject in [
            "orders.placed",
            "",
            "ns=x;s=a",
            "nsu=;s=a",
            "nsu=urn:a",
            "nsu=urn:a;ns=2;s=a",
        ] {
            let error = parse(subject).unwrap_err();
            assert!(matches!(error, StreamError::Unsupported(_)), "{subject}");
            assert!(error.to_string().contains("node id"), "{error}");
        }
    }
}
