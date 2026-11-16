//! Building a configuration from its layers.

use std::path::PathBuf;

use figment::{Figment, Provider};

use crate::{Config, Error, Profile, Source, env};

/// Builds a [`Config`] from an application's layers. See the [crate
/// documentation](crate) for the order they merge in.
pub struct Loader {
    prefix: String,
    profile_var: String,
    config_var: String,
    profile: Option<Profile>,
    release: Vec<Source>,
    dev_profile: Vec<Source>,
    dev: Vec<Source>,
    dev_path: Option<PathBuf>,
    files: Vec<Source>,
    discover: Vec<PathBuf>,
    providers: Vec<Box<dyn FnOnce(Figment) -> Figment>>,
    env: bool,
}

impl Loader {
    /// The loader of the application called name. Its environment variables
    /// start with the name upper-cased and an underscore (`acme` -> `ACME_`,
    /// `free-busy` -> `FREE_BUSY_`); `<PREFIX>PROFILE` picks the profile,
    /// `<PREFIX>CONFIG` names a file to read; the development profile's file
    /// is `<name>.dev.toml` in the working directory.
    pub fn new(name: &str) -> Loader {
        let prefix = format!(
            "{}_",
            name.to_ascii_uppercase().replace(['-', '.', ' '], "_")
        );
        Loader {
            profile_var: format!("{prefix}PROFILE"),
            config_var: format!("{prefix}CONFIG"),
            prefix,
            profile: None,
            release: Vec::new(),
            dev_profile: Vec::new(),
            dev: Vec::new(),
            dev_path: Some(PathBuf::from(format!("{name}.dev.toml"))),
            files: Vec::new(),
            discover: Vec::new(),
            providers: Vec::new(),
            env: true,
        }
    }

    /// Replaces the environment variable prefix (with its trailing
    /// underscore), and the profile and config variables named after it.
    pub fn prefix(mut self, prefix: &str) -> Loader {
        self.prefix = prefix.to_owned();
        self.profile_var = format!("{prefix}PROFILE");
        self.config_var = format!("{prefix}CONFIG");
        self
    }

    /// Adds a layer of release defaults. They merge in the order added, so an
    /// application's own document goes after the generated one.
    pub fn release(mut self, source: Source) -> Loader {
        self.release.push(source);
        self
    }

    /// Adds a layer of the development profile as compiled into the binary:
    /// what applies with `<PREFIX>PROFILE=dev` when the profile's file is not
    /// in the working directory, which replaces it when it is.
    pub fn dev_profile(mut self, source: Source) -> Loader {
        self.dev_profile.push(source);
        self
    }

    /// Adds a layer over the development profile, applied whenever a profile
    /// is (from its file or embedded): the application's own development
    /// settings.
    pub fn dev(mut self, source: Source) -> Loader {
        self.dev.push(source);
        self
    }

    /// Replaces the development profile's file (`<name>.dev.toml`); `None`
    /// reads no file, so only the embedded profile can apply.
    pub fn dev_path(mut self, path: Option<impl Into<PathBuf>>) -> Loader {
        self.dev_path = path.map(Into::into);
        self
    }

    /// Fixes the profile instead of reading `<PREFIX>PROFILE`.
    pub fn profile(mut self, profile: Profile) -> Loader {
        self.profile = Some(profile);
        self
    }

    /// Adds a file layer ([`Source::file`] or [`Source::optional_file`]).
    pub fn file(mut self, source: Source) -> Loader {
        self.files.push(source);
        self
    }

    /// Reads the first of paths that exists, if any, after the files added
    /// with [`file`](Loader::file): `acme.toml`, `acme.yaml`,
    /// `.config/acme.toml`, ...
    pub fn discover<I, P>(mut self, paths: I) -> Loader
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        self.discover.extend(paths.into_iter().map(Into::into));
        self
    }

    /// Merges a provider of the application's own -- a secret store, a remote
    /// config service -- after the files and before the environment.
    pub fn provider<P: Provider + 'static>(mut self, provider: P) -> Loader {
        self.providers.push(Box::new(move |f| f.merge(provider)));
        self
    }

    /// Reads no environment variables: for a test that must not see the
    /// developer's.
    pub fn without_env(mut self) -> Loader {
        self.env = false;
        self
    }

    /// Merges every layer.
    pub fn load(self) -> Result<Config, Error> {
        let profile = match self.profile {
            Some(p) => p,
            None => Profile::parse(
                &self.profile_var,
                &std::env::var(&self.profile_var).unwrap_or_default(),
            )?,
        };
        let mut files = Vec::new();
        let mut figment = Figment::new();

        // 1. The release defaults.
        for source in &self.release {
            figment = source.merge_into(figment, &mut files)?;
        }

        // 2. The development profile: its file when it is there, else the
        // embedded copy when asked for; then the layers over it.
        let dev_file = self.dev_path.as_ref().filter(|p| p.is_file());
        let profile_layers: Option<Vec<Source>> = match (profile, dev_file) {
            (Profile::Release, _) => None,
            (_, Some(path)) => Some(vec![Source::file(path.clone())?]),
            (Profile::Dev, None) => Some(self.dev_profile.clone()),
            (Profile::Auto, None) => None,
        };
        let dev_applied = profile_layers.is_some();
        if let Some(layers) = profile_layers {
            for source in layers.iter().chain(&self.dev) {
                figment = source.merge_into(figment, &mut files)?;
            }
        }

        // 3. Files: the ones added, the first discovered, then <PREFIX>CONFIG.
        for source in &self.files {
            figment = source.merge_into(figment, &mut files)?;
        }
        if let Some(path) = self.discover.iter().find(|p| p.is_file()) {
            figment = Source::file(path.clone())?.merge_into(figment, &mut files)?;
        }
        if let Some(path) = std::env::var_os(&self.config_var).filter(|v| !v.is_empty()) {
            figment = Source::file(PathBuf::from(path))?.merge_into(figment, &mut files)?;
        }

        // 4. The application's providers.
        for merge in self.providers {
            figment = merge(figment);
        }

        // 5. The environment, last so it wins.
        if self.env {
            figment = figment.merge(env::provider(
                &self.prefix,
                vec![self.profile_var, self.config_var],
            ));
        }

        Ok(Config {
            figment,
            profile,
            dev_applied,
            files,
        })
    }
}
