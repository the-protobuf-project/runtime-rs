//! Resources and prompts a generated server declares, and the placeholders it starts with.
//!
//! The proto states that a resource exists and what it is called; it cannot state what the
//! resource contains. So generated code registers these, and an implementation replaces the
//! ones it means to serve.

use rmcp::model::{
    ContentBlock, GetPromptResult, PromptMessage, ReadResourceResult, Resource, ResourceContents,
    Role,
};

/// The canonical `ui://` resource URI for a service's MCP App.
///
/// ```
/// use agents::mcp::app_resource_uri;
///
/// assert_eq!(app_resource_uri("TodoService"), "ui://todoservice/app.html");
/// ```
pub fn app_resource_uri(service_name: &str) -> String {
    format!("ui://{}/app.html", service_name.to_lowercase())
}

/// A resource result carrying an empty JSON object.
///
/// This is what a declared-but-unimplemented resource answers: valid, readable, and honest
/// about holding nothing.
pub fn default_resource_result(uri: &str) -> ReadResourceResult {
    ReadResourceResult::new(vec![ResourceContents::text("{}", uri)])
}

/// A minimal HTML page for an MCP App that has not been given a UI yet.
pub fn default_app_html(app_name: &str, version: &str, description: &str) -> String {
    format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head><meta charset=\"utf-8\"><title>{app_name}</title>\n\
<style>\n  body {{ font-family: system-ui, sans-serif; max-width: 600px; margin: 40px auto; padding: 0 20px; color: #333; }}\n\
  h1 {{ font-size: 1.5rem; }} p {{ color: #666; }} .version {{ font-size: 0.85rem; color: #999; }}\n\
</style>\n</head>\n<body>\n  <h1>{app_name}</h1>\n  <p class=\"version\">v{version}</p>\n  <p>{description}</p>\n\
  <p>This is a generated MCP App placeholder. Replace this resource with your own UI.</p>\n</body>\n</html>"
    )
}

/// The app resource result, serving [`default_app_html`] as `text/html`.
pub fn default_app_resource_result(
    uri: &str,
    app_name: &str,
    version: &str,
    description: &str,
) -> ReadResourceResult {
    ReadResourceResult::new(vec![
        ResourceContents::text(default_app_html(app_name, version, description), uri)
            .with_mime_type("text/html"),
    ])
}

/// The resource entry an MCP App is advertised as.
pub fn app_resource(service_name: &str) -> Resource {
    Resource::new(
        app_resource_uri(service_name),
        format!("{service_name} App"),
    )
    .with_mime_type("text/html")
}

/// A prompt result echoing the prompt's own description.
///
/// The placeholder a declared-but-unimplemented prompt answers with.
pub fn default_prompt_result(description: &str) -> GetPromptResult {
    GetPromptResult::new(vec![PromptMessage::new(
        Role::User,
        ContentBlock::text(description.to_string()),
    )])
    .with_description(description.to_string())
}
