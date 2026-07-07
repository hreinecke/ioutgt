# vmtest.sh — shared vmtest launcher config, sourced by testing/run_*.sh.
# Keeps the vmtest binary and its config file defined in one place instead of
# repeated inline in every launcher. Both honor a pre-set environment variable,
# so a single run can point elsewhere without editing this file:
#   VMTEST=/path/vmtest VMTEST_CONF=/path/vmtest.conf testing/run_interop.sh
VMTEST="${VMTEST:-$HOME/git/utils/vmtest/vmtest}"
VMTEST_CONF="${VMTEST_CONF:-$HOME/git/linux-ioutgt/vmtest.conf}"
