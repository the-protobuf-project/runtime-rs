//! The loader against real files and environment variables: every format
//! parses into the same typed configuration, and the layers override each
//! other in order -- the environment over files, files over the development
//! profile, the profile over the application's defaults.
//!
//! Each test runs in a figment::Jail: a temporary working directory, and
//! environment variables restored when it ends.

// figment::Jail fixes its closures' result to figment::error::Result, whose
// error is large; the loader's own Error boxes it.
#![allow(clippy::result_large_err)]

use config::{Error, Format, Loader, Profile, Source, Validate};
use figment::Jail;
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
struct App {
    name: String,
    server: Server,
    #[serde(default)]
    database: Database,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Server {
    host: String,
    port: u16,
    #[serde(default)]
    enable_mcp: bool,
    #[serde(default, deserialize_with = "config::de::comma_list")]
    allowed_origins: Vec<String>,
}

#[derive(Debug, Default, Deserialize, PartialEq)]
struct Database {
    #[serde(default)]
    secret_key: String,
}

impl Validate for Server {
    fn validate(&self) -> Result<(), String> {
        match self.port {
            0 => Err("server.port=0 (want a port to listen on)".into()),
            _ => Ok(()),
        }
    }
}

const RELEASE: &str = r#"
name = "acme"

[server]
host = "0.0.0.0"
port = 50051
"#;

/// Turns the loader's error into the jail's, so `?` works in a jail.
fn ok<T>(r: Result<T, Error>) -> figment::error::Result<T> {
    r.map_err(|e| e.to_string().into())
}

fn loader() -> Loader {
    Loader::new("acme").release(Source::toml(RELEASE))
}

#[test]
fn every_format_parses_to_the_same_config() {
    Jail::expect_with(|jail| {
        jail.create_file(
            "app.toml",
            "name = \"acme\"\n[server]\nhost = \"h\"\nport = 1\n",
        )?;
        jail.create_file("app.yaml", "name: acme\nserver:\n  host: h\n  port: 1\n")?;
        jail.create_file("app.yml", "name: acme\nserver:\n  host: h\n  port: 1\n")?;
        jail.create_file(
            "app.json",
            r#"{"name": "acme", "server": {"host": "h", "port": 1}}"#,
        )?;
        let want = App {
            name: "acme".into(),
            server: Server {
                host: "h".into(),
                port: 1,
                enable_mcp: false,
                allowed_origins: vec![],
            },
            database: Database::default(),
        };
        for file in ["app.toml", "app.yaml", "app.yml", "app.json"] {
            let app: App =
                ok(ok(Loader::new("acme").file(ok(Source::file(file))?).load())?.extract())?;
            assert_eq!(app, want, "{file}");
        }
        // Embedded documents, in each format.
        for source in [
            Source::toml("name = \"acme\"\n[server]\nhost = \"h\"\nport = 1\n"),
            Source::yaml("name: acme\nserver:\n  host: h\n  port: 1\n"),
            Source::json(r#"{"name": "acme", "server": {"host": "h", "port": 1}}"#),
        ] {
            let format = source.format();
            let app: App = ok(ok(Loader::new("acme").release(source).load())?.extract())?;
            assert_eq!(app, want, "{format:?}");
        }
        Ok(())
    });
}

#[test]
fn an_unknown_extension_is_refused() {
    assert!(matches!(
        Source::file("app.ini"),
        Err(Error::UnsupportedFormat(_))
    ));
    assert_eq!(Format::from_path("APP.YML".as_ref()).unwrap(), Format::Yaml);
}

#[test]
fn env_overrides_files_override_the_profile_overrides_the_defaults() {
    Jail::expect_with(|jail| {
        // Each layer sets the port; the highest one present wins.
        jail.create_file("acme.dev.toml", "[server]\nport = 2\nhost = \"dev\"\n")?;
        jail.create_file("acme.yaml", "server:\n  port: 3\n")?;
        let port =
            |l: Loader| -> figment::error::Result<u16> { ok(ok(l.load())?.section("server.port")) };

        assert_eq!(
            port(loader().profile(Profile::Release))?,
            50051,
            "the defaults alone"
        );
        assert_eq!(port(loader())?, 2, "the profile over the defaults");
        assert_eq!(
            port(loader().discover(["acme.toml", "acme.yaml"]))?,
            3,
            "a file over the profile"
        );
        jail.set_env("ACME_SERVER__PORT", "4");
        assert_eq!(
            port(loader().discover(["acme.yaml"]))?,
            4,
            "the environment over everything"
        );

        // A layer overrides keys, not sections: the profile's host stays.
        let app: App = ok(ok(loader().discover(["acme.yaml"]).load())?.extract())?;
        assert_eq!((app.server.host.as_str(), app.server.port), ("dev", 4));
        Ok(())
    });
}

#[test]
fn the_config_variable_names_a_file() {
    Jail::expect_with(|jail| {
        jail.create_file("prod.json", r#"{"server": {"port": 7}}"#)?;
        jail.set_env("ACME_CONFIG", "prod.json");
        let config = ok(loader().load())?;
        assert_eq!(ok(config.section::<u16>("server.port"))?, 7);
        assert_eq!(
            config.files().collect::<Vec<_>>(),
            vec![std::path::Path::new("prod.json")]
        );
        // ACME_CONFIG steers the load; it is not itself a key.
        assert!(!config.contains("config"));

        jail.set_env("ACME_CONFIG", "missing.toml");
        assert!(matches!(loader().load(), Err(Error::MissingFile(_))));
        Ok(())
    });
}

#[test]
fn the_development_profile() {
    Jail::expect_with(|jail| {
        let embedded = || loader().dev_profile(Source::toml("[server]\nport = 9\n"));
        let port = |l: Loader| -> figment::error::Result<(u16, bool)> {
            let c = ok(l.load())?;
            Ok((ok(c.section("server.port"))?, c.is_dev()))
        };
        // No file: the embedded profile only when asked for.
        assert_eq!(port(embedded())?, (50051, false));
        jail.set_env("ACME_PROFILE", "dev");
        assert_eq!(port(embedded())?, (9, true));
        // Its file, when there, replaces the embedded copy.
        jail.create_file("acme.dev.toml", "[server]\nport = 8\n")?;
        assert_eq!(port(embedded())?, (8, true));
        jail.set_env("ACME_PROFILE", "");
        assert_eq!(port(embedded())?, (8, true));
        // Release loads none, even in a checkout.
        jail.set_env("ACME_PROFILE", "release");
        assert_eq!(port(embedded())?, (50051, false));
        // Layers over the profile apply whenever it does.
        jail.set_env("ACME_PROFILE", "dev");
        assert_eq!(
            port(embedded().dev(Source::toml("[server]\nport = 10\n")))?,
            (10, true)
        );

        jail.set_env("ACME_PROFILE", "staging");
        assert!(matches!(embedded().load(), Err(Error::Profile { .. })));
        Ok(())
    });
}

#[test]
fn environment_variables_are_typed_keys() {
    Jail::expect_with(|jail| {
        jail.set_env("ACME_SERVER__ENABLE_MCP", "true");
        jail.set_env(
            "ACME_SERVER__ALLOWED_ORIGINS",
            "https://a.example, https://b.example",
        );
        jail.set_env("ACME_DATABASE__SECRET_KEY", "s3cret");
        jail.set_env("OTHER_SERVER__PORT", "1");
        let app: App = ok(ok(loader().load())?.extract())?;
        assert!(app.server.enable_mcp);
        assert_eq!(
            app.server.allowed_origins,
            ["https://a.example", "https://b.example"]
        );
        // A double underscore separates levels, so a key keeps its own.
        assert_eq!(app.database.secret_key, "s3cret");
        assert_eq!(app.server.port, 50051, "another prefix is not read");

        // Without a double underscore, every underscore is a level.
        jail.set_env("ACME_SERVER_PORT", "5");
        assert_eq!(ok(ok(loader().load())?.section::<u16>("server.port"))?, 5);
        // without_env reads none.
        assert_eq!(
            ok(ok(loader().without_env().load())?.section::<u16>("server.port"))?,
            50051
        );
        Ok(())
    });
}

#[test]
fn lists_read_from_files_too() {
    Jail::expect_with(|_| {
        let source =
            Source::toml("[server]\nhost = \"h\"\nport = 1\nallowed_origins = [\"a\", \"b, c\"]\n");
        let server: Server = ok(ok(Loader::new("acme").release(source).load())?.section("server"))?;
        assert_eq!(server.allowed_origins, ["a", "b", "c"]);
        Ok(())
    });
}

#[test]
fn bad_configuration_is_refused_naming_the_key() {
    Jail::expect_with(|jail| {
        jail.set_env("ACME_SERVER__PORT", "0");
        let err = ok(loader().load())?
            .section_validated::<Server>("server")
            .unwrap_err();
        assert!(
            matches!(&err, Error::Invalid(m) if m.contains("server.port=0")),
            "{err}"
        );

        jail.set_env("ACME_SERVER__PORT", "not-a-port");
        let err = ok(loader().load())?
            .section::<Server>("server")
            .unwrap_err()
            .to_string();
        assert!(err.contains("port"), "{err}");

        jail.create_file("broken.toml", "[server\nport = 1")?;
        let err = loader()
            .file(ok(Source::file("broken.toml"))?)
            .load()
            .and_then(|c| c.extract::<App>());
        assert!(matches!(err, Err(Error::Figment(_))), "{err:?}");
        Ok(())
    });
}

#[test]
fn optional_files_and_defaults() {
    Jail::expect_with(|_| {
        let config = ok(loader()
            .file(ok(Source::optional_file("absent.toml"))?)
            .load())?;
        assert_eq!(config.files().count(), 0);
        assert!(matches!(
            loader().file(ok(Source::file("absent.toml"))?).load(),
            Err(Error::MissingFile(_))
        ));
        // A section nothing sets reads as its default.
        assert_eq!(
            ok(config.section_or_default::<Database>("database"))?,
            Database::default()
        );
        Ok(())
    });
}

#[test]
fn global_installs_once() {
    let first = config::Loader::new("acme")
        .release(Source::toml(RELEASE))
        .without_env()
        .load()
        .unwrap();
    let installed = config::global::install(first).unwrap();
    assert_eq!(installed.section::<String>("name").unwrap(), "acme");
    let second = config::Loader::new("acme").without_env().load().unwrap();
    assert!(matches!(
        config::global::install(second),
        Err(Error::AlreadyInstalled)
    ));
    assert!(config::global::get().is_some());
}
