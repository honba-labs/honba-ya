//! `ProfileStore`: isolation, revisions, id validation and atomic writes.

use serde_json::json;

use crate::{Profile, ProfileError, ProfileStore};

fn store() -> (tempfile::TempDir, ProfileStore) {
    let tmp = tempfile::tempdir().unwrap();
    let store = ProfileStore::new(tmp.path().join("profiles"));
    (tmp, store)
}

fn with_shortlist(symbols: &[&str]) -> Profile {
    Profile {
        shortlist: symbols.iter().map(|s| s.to_string()).collect(),
        ..Profile::default()
    }
}

#[test]
fn a_missing_user_gets_the_default_profile() {
    let (_tmp, store) = store();
    let profile = store.get("default").unwrap();
    assert_eq!(profile, Profile::default());
    assert_eq!(profile.revision, 0);
}

#[test]
fn put_stores_the_profile_and_bumps_the_revision() {
    let (_tmp, store) = store();
    let mut p = with_shortlist(&["INFY"]);
    p.preferences = json!({"theme": "dark"});
    let first = store.put("alice", 0, p).unwrap();
    assert_eq!(first.revision, 1);
    let second = store.put("alice", 1, with_shortlist(&["TCS"])).unwrap();
    assert_eq!(second.revision, 2);
    let got = store.get("alice").unwrap();
    assert_eq!(got, second);
    assert_eq!(got.shortlist, vec!["TCS".to_string()]);
}

#[test]
fn a_stale_revision_is_a_conflict_and_changes_nothing() {
    let (_tmp, store) = store();
    store.put("alice", 0, with_shortlist(&["A"])).unwrap();
    let err = store.put("alice", 0, with_shortlist(&["B"])).unwrap_err();
    assert!(matches!(
        err,
        ProfileError::RevisionConflict {
            expected: 0,
            current: 1
        }
    ));
    assert_eq!(store.get("alice").unwrap().shortlist, vec!["A".to_string()]);
}

#[test]
fn users_are_isolated() {
    let (_tmp, store) = store();
    store.put("alice", 0, with_shortlist(&["A"])).unwrap();
    store.put("bob", 0, with_shortlist(&["B"])).unwrap();
    assert_eq!(store.get("alice").unwrap().shortlist, vec!["A".to_string()]);
    assert_eq!(store.get("bob").unwrap().shortlist, vec!["B".to_string()]);
    assert_eq!(store.get("carol").unwrap().revision, 0);
}

#[test]
fn hostile_or_malformed_user_ids_are_rejected() {
    let (tmp, store) = store();
    let too_long = "a".repeat(65);
    for bad in [
        "../x",
        "",
        "a/b",
        "a\\b",
        "a.b",
        "..",
        "a b",
        "é",
        too_long.as_str(),
    ] {
        assert!(
            matches!(store.get(bad), Err(ProfileError::InvalidUserId)),
            "get {bad:?}"
        );
        assert!(
            matches!(
                store.put(bad, 0, Profile::default()),
                Err(ProfileError::InvalidUserId)
            ),
            "put {bad:?}"
        );
    }
    assert!(!tmp.path().join("profiles").exists());
    let longest = "a".repeat(64);
    store.put(&longest, 0, Profile::default()).unwrap();
    store.put("Ab_9-z", 0, Profile::default()).unwrap();
}

#[test]
fn put_leaves_one_file_and_no_temp_files() {
    let (tmp, store) = store();
    store.put("alice", 0, with_shortlist(&["A"])).unwrap();
    store.put("alice", 1, with_shortlist(&["B"])).unwrap();
    let names: Vec<String> = std::fs::read_dir(tmp.path().join("profiles"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, vec!["alice.json".to_string()]);
}

#[test]
fn alerts_default_when_absent_from_old_json() {
    let old = r#"{"revision": 3, "shortlist": ["A"]}"#;
    let profile: Profile = serde_json::from_str(old).unwrap();
    assert_eq!(profile.revision, 3);
    assert_eq!(profile.alerts, serde_json::Value::Null);
}

#[test]
fn alerts_round_trip_through_the_store() {
    let (_tmp, store) = store();
    let p = Profile {
        alerts: json!({"rules": [{"id": "r1"}]}),
        ..Profile::default()
    };
    store.put("alice", 0, p).unwrap();
    assert_eq!(
        store.get("alice").unwrap().alerts,
        json!({"rules": [{"id": "r1"}]})
    );
}

#[test]
fn update_bumps_the_revision_once_and_persists() {
    let (_tmp, store) = store();
    let after = store
        .update("alice", |p| {
            p.shortlist.push("A".into());
            p.shortlist.push("B".into());
            true
        })
        .unwrap();
    assert_eq!(after.revision, 1);
    assert_eq!(store.get("alice").unwrap(), after);
}

#[test]
fn update_that_changes_nothing_writes_nothing() {
    let (tmp, store) = store();
    let after = store.update("alice", |_| false).unwrap();
    assert_eq!(after.revision, 0);
    assert!(!tmp.path().join("profiles").exists());
}

#[test]
fn update_rejects_a_hostile_user_id() {
    let (tmp, store) = store();
    assert!(matches!(
        store.update("../x", |_| true),
        Err(ProfileError::InvalidUserId)
    ));
    assert!(!tmp.path().join("profiles").exists());
}

#[test]
fn count_limits_are_refused_and_change_nothing() {
    use crate::profile_store::{MAX_LAYOUTS, MAX_MODULES, MAX_SYMBOLS, MAX_WATCHLISTS};

    let (_tmp, store) = store();
    let mut layouts = Profile::default();
    for i in 0..=MAX_LAYOUTS {
        layouts.layouts.insert(format!("l{i}"), json!(1));
    }
    let mut modules = Profile::default();
    for i in 0..=MAX_MODULES {
        modules
            .modules
            .insert(format!("m{i}"), crate::Module::default());
    }
    let mut lists = Profile::default();
    for i in 0..=MAX_WATCHLISTS {
        lists.watchlists.insert(format!("w{i}"), vec![]);
    }
    let long = vec!["X".to_string(); MAX_SYMBOLS + 1];
    let mut long_list = Profile::default();
    long_list.watchlists.insert("big".into(), long.clone());
    let long_shortlist = Profile {
        shortlist: long,
        ..Profile::default()
    };
    for (name, bad) in [
        ("layouts", layouts),
        ("modules", modules),
        ("watchlists", lists),
        ("watchlist symbols", long_list),
        ("shortlist", long_shortlist),
    ] {
        let err = store.put("alice", 0, bad).unwrap_err();
        assert!(matches!(err, ProfileError::TooLarge { .. }), "{name}");
    }
    assert_eq!(store.get("alice").unwrap().revision, 0);
    // Exactly at a limit is fine.
    let mut ok = Profile::default();
    for i in 0..MAX_LAYOUTS {
        ok.layouts.insert(format!("l{i}"), json!(1));
    }
    store.put("alice", 0, ok).unwrap();
}

#[test]
fn an_oversized_profile_is_refused_by_put_and_update() {
    use crate::profile_store::MAX_PROFILE_BYTES;

    let (_tmp, store) = store();
    let blob = json!("x".repeat(MAX_PROFILE_BYTES));
    let big = Profile {
        preferences: blob.clone(),
        ..Profile::default()
    };
    assert!(matches!(
        store.put("alice", 0, big),
        Err(ProfileError::TooLarge { .. })
    ));
    assert!(matches!(
        store.update("alice", |p| {
            p.layouts.insert("big".into(), blob);
            true
        }),
        Err(ProfileError::TooLarge { .. })
    ));
    assert_eq!(store.get("alice").unwrap().revision, 0);
}

#[test]
fn a_corrupt_file_is_moved_aside_and_the_user_starts_fresh() {
    let (tmp, store) = store();
    let dir = tmp.path().join("profiles");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("alice.json"), b"{ not json").unwrap();

    let got = store.get("alice").unwrap();
    assert_eq!(got, Profile::default());
    assert!(!dir.join("alice.json").exists());
    let aside: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(aside.len(), 1, "{aside:?}");
    assert!(aside[0].starts_with("alice.json.corrupt-"), "{aside:?}");
    assert_eq!(std::fs::read(dir.join(&aside[0])).unwrap(), b"{ not json");

    // Writes work again from revision 0.
    let put = store.put("alice", 0, with_shortlist(&["A"])).unwrap();
    assert_eq!(put.revision, 1);
}

#[test]
fn concurrent_updates_on_one_user_all_land() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 25;
    let (_tmp, store) = store();
    let store = std::sync::Arc::new(store);
    let workers: Vec<_> = (0..THREADS)
        .map(|t| {
            let store = store.clone();
            std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    store
                        .update("alice", |p| {
                            p.shortlist.push(format!("{t}-{i}"));
                            true
                        })
                        .unwrap();
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    let profile = store.get("alice").unwrap();
    assert_eq!(profile.revision, (THREADS * PER_THREAD) as u64);
    assert_eq!(profile.shortlist.len(), THREADS * PER_THREAD);
}

#[test]
fn temp_names_are_unique_per_write_and_cleaned_up() {
    let (tmp, store) = store();
    let dir = tmp.path().join("profiles");
    store.put("alice", 0, with_shortlist(&["A"])).unwrap();
    // A stale temp file from a crashed writer does not block later writes.
    std::fs::write(dir.join("alice.json.tmp"), b"junk").unwrap();
    store.put("alice", 1, with_shortlist(&["B"])).unwrap();
    store.put("alice", 2, with_shortlist(&["C"])).unwrap();
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, vec!["alice.json", "alice.json.tmp"]);
    assert_eq!(std::fs::read(dir.join("alice.json.tmp")).unwrap(), b"junk");
}

#[test]
fn a_write_into_a_new_nested_directory_is_durable_and_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let store = ProfileStore::new(tmp.path().join("a").join("b").join("profiles"));
    store.put("alice", 0, with_shortlist(&["A"])).unwrap();
    store.put("alice", 1, with_shortlist(&["B"])).unwrap();
    assert_eq!(store.get("alice").unwrap().shortlist, vec!["B".to_string()]);
    crate::user_doc::sync_dir(tmp.path()).unwrap();
    #[cfg(unix)]
    assert!(crate::user_doc::sync_dir(&tmp.path().join("missing")).is_err());
}

#[test]
fn unknown_fields_round_trip_through_get_put_and_update() {
    let (tmp, store) = store();
    let dir = tmp.path().join("profiles");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("alice.json"),
        r#"{"revision": 1, "shortlist": ["A"], "futureThing": {"k": [1, 2]}}"#,
    )
    .unwrap();

    let got = store.get("alice").unwrap();
    assert_eq!(got.extra["futureThing"], json!({"k": [1, 2]}));

    // A client that read it sends it back; the field survives a put ...
    let mut next = got.clone();
    next.shortlist.push("B".into());
    let put = store.put("alice", 1, next).unwrap();
    assert_eq!(put.extra["futureThing"], json!({"k": [1, 2]}));

    // ... and a sub-resource update, which never sees the field, keeps it too.
    store
        .update("alice", |p| {
            p.layouts.insert("main".into(), json!({}));
            true
        })
        .unwrap();
    let disk: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("alice.json")).unwrap()).unwrap();
    assert_eq!(disk["futureThing"], json!({"k": [1, 2]}));
    assert_eq!(disk["revision"], json!(3));
}

#[test]
fn putting_an_unchanged_profile_keeps_the_revision_and_writes_nothing() {
    let (tmp, store) = store();
    let first = store.put("alice", 0, with_shortlist(&["A"])).unwrap();
    assert_eq!(first.revision, 1);
    let path = tmp.path().join("profiles/alice.json");
    let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();

    // Same content at the current revision: the stored profile comes back untouched.
    let same = store.put("alice", 1, with_shortlist(&["A"])).unwrap();
    assert_eq!(same, first);
    assert_eq!(store.get("alice").unwrap().revision, 1);
    assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), mtime);

    // A real change still bumps it, and a stale revision is still a conflict.
    assert_eq!(
        store
            .put("alice", 1, with_shortlist(&["B"]))
            .unwrap()
            .revision,
        2
    );
    assert!(matches!(
        store.put("alice", 1, with_shortlist(&["B"])),
        Err(ProfileError::RevisionConflict { .. })
    ));
}
