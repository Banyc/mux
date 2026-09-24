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
    /// The package naming the transport layer this crate must not depend on.
    const TRANSPORT_PACKAGE: &str = "rtp";

    /// The TOML tables whose keys name the transport package. A dependency
    /// table's key is an edge on it; a `patch` table's key is the package being
    /// redirected. Either is a reference to the transport layer. `[features]`,
    /// `[package]` and the rest are not: a feature or a description that merely
    /// reuses the package name references nothing.
    const REFERENCING_TABLES: [&str; 4] = [
        "dependencies",
        "dev-dependencies",
        "build-dependencies",
        "patch",
    ];

    /// The manifest with every comment removed. A comment starts at the first
    /// `#` outside a quoted string, so the relocation notes the manifest keeps
    /// (`# rtp = { … }`) are not read as declarations while a `#` inside a
    /// value (`git = "…#fragment"`) is not read as a comment.
    fn without_comments(manifest: &str) -> String {
        manifest
            .lines()
            .map(comment_free_prefix)
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn comment_free_prefix(line: &str) -> &str {
        let mut quote: Option<char> = None;
        let mut escaped = false;
        for (index, character) in line.char_indices() {
            match quote {
                Some(open) => {
                    if open == '"' && character == '\\' && !escaped {
                        escaped = true;
                        continue;
                    }
                    if character == open && !escaped {
                        quote = None;
                    }
                    escaped = false;
                }
                None => match character {
                    '"' | '\'' => quote = Some(character),
                    '#' => return &line[..index],
                    _ => {}
                },
            }
        }
        line
    }

    /// The key part of a `key = value` line, or `None` when the line is not a
    /// key/value pair. A TOML key contains no bare `=`, so the first `=` outside
    /// a quoted key is the separator.
    fn key_of(line: &str) -> Option<&str> {
        let mut quote: Option<char> = None;
        for (index, character) in line.char_indices() {
            match quote {
                Some(open) => {
                    if character == open {
                        quote = None;
                    }
                }
                None => match character {
                    '"' | '\'' => quote = Some(character),
                    '=' => return Some(line[..index].trim()),
                    _ => {}
                },
            }
        }
        None
    }

    /// Splits a dotted TOML path into its unquoted segments, so
    /// `dependencies."rtp"`, `dependencies.rtp` and `[target.'cfg(unix)'.…]`
    /// all yield the bare key names.
    fn path_segments(path: &str) -> Vec<String> {
        let mut segments = Vec::new();
        let mut current = String::new();
        let mut quote: Option<char> = None;
        for character in path.chars() {
            match quote {
                Some(open) if character == open => quote = None,
                Some(_) => current.push(character),
                None => match character {
                    '"' | '\'' => quote = Some(character),
                    '.' => {
                        segments.push(current.trim().to_owned());
                        current.clear();
                    }
                    _ => current.push(character),
                },
            }
        }
        segments.push(current.trim().to_owned());
        segments
    }

    /// True when `path` names the transport package where a manifest refers to
    /// it as a dependency: the package is the whole leading segment (a stray
    /// top-level `rtp` entry or `[rtp]` table), or a preceding segment is a
    /// dependency or `patch` table. A `[features]` entry that merely *reuses*
    /// the package name reaches neither, so it never trips the guard.
    fn path_reaches_transport(path: &[String]) -> bool {
        let Some(package) = path
            .iter()
            .position(|segment| segment.as_str() == TRANSPORT_PACKAGE)
        else {
            return false;
        };
        package == 0
            || path[..package]
                .iter()
                .any(|segment| REFERENCING_TABLES.contains(&segment.as_str()))
    }

    /// Every place the manifest actively reaches the transport layer: an entry
    /// naming the package under a dependency or `patch` table (in any of its
    /// spellings — inline key, quoted key, dotted key, or a table header, under
    /// any declared target/dev/build table), or a feature list activating it
    /// through `dep:rtp`. `dep:rtp` is matched on active text only: the token is
    /// meaningful in a feature list, and matching it anywhere in the document
    /// would fail the guard on the commented relocation note.
    fn transport_references(manifest: &str) -> Vec<String> {
        let text = without_comments(manifest);
        let mut findings = Vec::new();
        if text.contains("dep:rtp") {
            findings.push("dep:rtp feature activation".to_owned());
        }
        let mut section: Vec<String> = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if line.starts_with('[') {
                section = path_segments(line.trim_start_matches('[').trim_end_matches(']'));
                if path_reaches_transport(&section) {
                    findings.push(format!("table [{}]", section.join(".")));
                }
                continue;
            }
            let Some(key) = key_of(line) else {
                continue;
            };
            let mut path = section.clone();
            path.extend(path_segments(key));
            if path_reaches_transport(&path) {
                findings.push(format!("entry {} = …", path.join(".")));
            }
        }
        findings
    }

    /// The mux layer must not know the transport layer. Any active transport
    /// dependency — not the commented relocation note the manifest keeps, which
    /// names every spelling the guard has to ignore — would let mux code reach
    /// the transport's items and defeat the layering the cooperation crate
    /// relies on, so the manifest itself is the thing asserted. `cargo test -p
    /// mux` enables `testing` through the self dev-dependency, so this runs in
    /// the default gate.
    #[test]
    fn manifest_declares_no_active_transport_dependency() {
        let manifest = include_str!("../../Cargo.toml");
        // The guard reads a manifest that could carry the dependency: a stale
        // or missing path would pass the check below vacuously.
        assert!(
            manifest.contains("name = \"mux\""),
            "the guard must read the mux package manifest"
        );
        let findings = transport_references(manifest);
        assert!(
            findings.is_empty(),
            "mux must not depend on the transport layer; active entries: {findings:?}"
        );
    }

    /// Every spelling a manifest can use to declare the dependency must be
    /// caught. The checked property is the presence of a reference to the
    /// package, not one textual shape of it, so the guard has to read the TOML
    /// structure: an inline key, a quoted key, a dotted key, a table header, a
    /// `patch` entry, and the same under a declared target/`dev`/`build` table.
    #[test]
    fn manifest_guard_catches_every_dependency_spelling() {
        let declarations = [
            "[dependencies]\nrtp = { git = \"https://github.com/Banyc/rtp.git\" }\n",
            "[dependencies]\n\"rtp\" = { path = \"../rtp\" }\n",
            "[dependencies]\n'rtp' = { path = \"../rtp\" }\n",
            "[dependencies.rtp]\ngit = \"https://github.com/Banyc/rtp.git\"\n",
            "[dependencies.\"rtp\"]\ngit = \"https://github.com/Banyc/rtp.git\"\n",
            "[dev-dependencies.rtp]\ngit = \"https://github.com/Banyc/rtp.git\"\n",
            "[dev-dependencies]\nrtp = { path = \"../rtp\" }\n",
            "[build-dependencies.rtp]\npath = \"../rtp\"\n",
            "[target.'cfg(unix)'.dependencies.rtp]\ngit = \"https://github.com/Banyc/rtp.git\"\n",
            "[target.'cfg(target_os = \"linux\")'.dependencies.rtp]\npath = \"../rtp\"\n",
            "[target.x86_64-unknown-linux-gnu.dev-dependencies.rtp]\ngit = \"x\"\n",
            "[target.'cfg(unix)'.dependencies]\nrtp = { path = \"../rtp\" }\n",
            "dependencies.rtp = { path = \"../rtp\" }\n",
            "[dependencies]\nrtp.git = \"https://github.com/Banyc/rtp.git\"\n",
            "[features]\ntesting = [\"dep:rtp\"]\n",
            "[features]\ntesting = [\n    \"dep:rtp\",\n]\n",
            "[patch.crates-io]\nrtp = { path = \"../rtp\" }\n",
            "[patch.crates-io.rtp]\npath = \"../rtp\"\n",
            // A stray top-level entry or `[rtp]` table declares no edge, but
            // the prefix filter the guard used to be caught them; keep
            // catching them so the guard only ever widens.
            "rtp = \"1.0\"\n",
            "[rtp]\ngit = \"https://github.com/Banyc/rtp.git\"\n",
        ];
        for declaration in declarations {
            let findings = transport_references(declaration);
            assert!(
                !findings.is_empty(),
                "the transport-dependency guard must catch this manifest:\n{declaration}"
            );
        }
    }

    /// The negative controls: text that names the transport package without
    /// declaring a dependency on it must keep passing, including the commented
    /// relocation note the real manifest keeps.
    #[test]
    fn manifest_guard_ignores_mentions_that_are_not_dependencies() {
        let mentions = [
            "[dependencies]\n# rtp = { git = \"…\", optional = true }\n",
            "# [dependencies.rtp]\n# git = \"…\"\n",
            "[features]\n# testing = [\"dep:rtp\"]\n",
            "# rtp is deliberately absent here\n",
            "[package]\ndescription = \"mux, but without rtp\"\n",
            "[dependencies]\nrtp-fec = { path = \"../rtp-fec\" }\n",
        ];
        for mention in mentions {
            let findings = transport_references(mention);
            assert!(
                findings.is_empty(),
                "the guard must not flag this manifest, found {findings:?}:\n{mention}"
            );
        }

        // A feature whose name reuses the package name is not a dependency
        // edge: the feature table is not a dependency table, so the guard must
        // let it through (the checked property is the edge, not the word).
        let feature_named_after_the_package = "[features]\nrtp = []\n";
        assert!(
            transport_references(feature_named_after_the_package).is_empty(),
            "a feature named rtp is not a dependency on rtp"
        );
    }
}
