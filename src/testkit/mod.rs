//! mux layer-testing kit: the transport-free client-connect and session
//! supervision helpers used by the mux scenario suites, compiled behind the
//! `testing` feature.
//!
//! This is the owning crate's half of the shared scenario scaffolding. The
//! generic helpers (payload, task scopes, reporting, presets) live in the
//! `netem-test` harness kit (`netem_test::kit`, behind its `test-kit`
//! feature) and are consumed here; the transport-mediated `mux`-over-`rtp`
//! scaffolding lives in the cooperation crate's kit, which is the one place
//! that sees both layers together. Imports only ever go downward (mux kit
//! → harness kit), so `netem-test` stays a leaf.
//!
//! The operator's product constitution is stated in `GATE.md`
//! ("Performance"); the topology-level gates that need both layers are owned
//! by the crate that owns the dual-lane topology.

pub mod mux;
pub mod stats;

#[cfg(test)]
mod dependency_boundary_tests {
    /// The mux layer must not know the transport layer. A re-added transport
    /// dependency (active, not the commented relocation note the manifest
    /// keeps in `[dependencies]`) would let mux code reach the transport's
    /// items and defeat the layering the cooperation crate relies on, so the
    /// manifest itself is the thing asserted. `cargo test -p mux` enables
    /// `testing` through the self dev-dependency, so this runs in the default
    /// gate.
    #[test]
    fn manifest_declares_no_active_transport_dependency() {
        let manifest = include_str!("../../Cargo.toml");
        let active: Vec<&str> = manifest
            .lines()
            .map(str::trim)
            .filter(|line| !line.starts_with('#') && line.starts_with("rtp"))
            .collect();
        assert!(
            active.is_empty(),
            "mux must not depend on the transport layer; active entries: {active:?}"
        );
        assert!(
            !manifest.contains("dep:rtp"),
            "mux's testing feature must not activate a transport dependency"
        );
    }
}
