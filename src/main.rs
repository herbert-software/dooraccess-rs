fn main() {
    println!("dooraccess-rs phase0 probe");
    // Self-report RSS/VmHWM so the hAP run can record memory without racing
    // an external /proc read against an instantly-exiting process (spec 4.5).
    // On non-Linux hosts /proc is absent; that branch is simply skipped.
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if line.starts_with("VmRSS") || line.starts_with("VmHWM") || line.starts_with("VmPeak")
            {
                println!("{line}");
            }
        }
    }
}
