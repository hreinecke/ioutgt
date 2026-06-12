//! The running kernel must support everything phase 1 relies on.

#[test]
fn kernel_supports_phase1_features() {
    let features = ioutgt_uring::probe().expect("io_uring probe failed");
    eprintln!("io_uring features: {features:?}");
    assert!(
        features.phase1_ok(),
        "kernel lacks required io_uring features: {features:?}"
    );
}

#[test]
fn sendmsg_zc_supported_on_project_floor() {
    let features = ioutgt_uring::probe().unwrap();
    // SENDMSG_ZC is kernel 6.1+; the project floor is 6.11.
    assert!(features.sendmsg_zc);
}
