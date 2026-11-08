This is the crates.io source for `rumqttc` 0.25.1 (Apache-2.0).

The only change to the published crate is raising its `rustls-webpki`
dependency from `0.102.8` to `0.103.16` in both manifest copies. The old 0.102.x line contains
RUSTSEC-2026-0049, RUSTSEC-2026-0098, RUSTSEC-2026-0099,
RUSTSEC-2026-0104, and RUSTSEC-2026-0382. The patch can be removed when
upstream publishes a compatible fixed release.

Cargo applies this root-level patch to this workspace only. A separately
published `tpp-streams` crate still resolves the upstream `rumqttc` release;
its consumers need the same patch or a future upstream release.
