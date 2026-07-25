//! End-to-end GraphQL example against the public Rick and Morty API
//! (<https://rickandmortyapi.com/graphql>, no auth required) — the same service the Go package's
//! examples use. Run with:
//!
//! ```text
//! cargo run -p network --example rickandmorty_graphql
//! ```

use std::collections::HashMap;

use network::{ConnectionOptions, FieldArg, GraphQLClient, UrlOptions, UrlScheme};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize, Debug)]
struct Character {
    name: String,
    status: String,
    species: String,
    gender: String,
}

#[derive(Deserialize, Debug)]
struct Episode {
    name: String,
    episode: String,
}

#[derive(Deserialize, Debug)]
struct CharacterWithEpisodes {
    name: String,
    episode: Vec<Episode>,
}

#[derive(Deserialize, Debug)]
struct CharacterSummary {
    id: String,
    name: String,
    status: String,
    species: String,
}

#[derive(Deserialize, Debug)]
struct CharactersPage {
    results: Vec<CharacterSummary>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== GraphQL Client Examples ===");
    println!("Using Rick and Morty GraphQL API\n");

    let mut gql = GraphQLClient::default();
    gql.connect(ConnectionOptions {
        url: UrlOptions {
            scheme: UrlScheme::Https,
            host: "rickandmortyapi.com".to_string(),
            paths: vec!["/graphql".to_string()],
            params: HashMap::new(),
        },
        ..Default::default()
    })
    .await?;
    println!("Connected to GraphQL server successfully!\n");

    // 1. Typed query (network::GraphQLClient::query) — caller-supplied query text, decoded into T.
    println!("1. Query - Get Character by ID (typed `query`)");
    #[derive(Deserialize)]
    struct CharacterData {
        character: Character,
    }
    let data: CharacterData = gql
        .query(
            r#"query { character(id: 1) { name status species gender } }"#,
            None,
        )
        .await?;
    println!("   Name: {}", data.character.name);
    println!("   Status: {}", data.character.status);
    println!("   Species: {}", data.character.species);
    println!("   Gender: {}\n", data.character.gender);

    // 2. Dynamic field-based query (query_fields) with a variable argument.
    println!("2. Query - Character with Episodes (dynamic `query_fields`)");
    let with_episodes: CharacterWithEpisodes = gql
        .query_fields(
            "character",
            &[FieldArg::new("id", 1, "ID!")],
            "{ name episode { name episode } }",
        )
        .await?;
    println!("   Name: {}", with_episodes.name);
    println!(
        "   Appears in {} episodes; first three:",
        with_episodes.episode.len()
    );
    for ep in with_episodes.episode.iter().take(3) {
        println!("     - {} ({})", ep.name, ep.episode);
    }
    println!();

    // 3. Raw query (execute_raw_query) with a filter argument, returning an untyped JSON map.
    println!("3. Query - Search by Name (raw `execute_raw_query`)");
    let mut variables = serde_json::Map::new();
    variables.insert("name".to_string(), json!("Rick"));
    let raw = gql
        .execute_raw_query(
            r#"query($name: String!) { characters(filter: { name: $name }) { results { id name status } } }"#,
            Some(&serde_json::Value::Object(variables)),
        )
        .await?;
    let count = raw["characters"]["results"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    println!("   SUCCESS: {count} characters matching \"Rick\"");
    if let Some(first) = raw["characters"]["results"].get(0) {
        println!("   First match: {} ({})", first["name"], first["status"]);
    }
    println!();

    // 4. Typed query - list all characters (first page).
    println!("4. Query - Get All Characters (first page, typed `query`)");
    #[derive(Deserialize)]
    struct CharactersData {
        characters: CharactersPage,
    }
    let page: CharactersData = gql
        .query(
            "query { characters { results { id name status species } } }",
            None,
        )
        .await?;
    println!(
        "   SUCCESS: Found {} characters on this page",
        page.characters.results.len()
    );
    for c in page.characters.results.iter().take(5) {
        println!("   - #{} {} ({}, {})", c.id, c.name, c.status, c.species);
    }
    println!("   ...\n");

    println!("All GraphQL examples completed successfully.");
    Ok(())
}
