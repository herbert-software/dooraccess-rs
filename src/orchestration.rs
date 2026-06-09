//! daemon 编排 helper（纯逻辑接线，从 `main.rs` 上移到 lib crate，`port-rust-daemon-skeleton`
//! G4 FIX 问题 B）。
//!
//! 这些是 `run()` 的**纯逻辑 orchestration 接线**——automation flag 三级优先级加载、OnDetect
//! 门铃 builder、号码查询 callback builder、手动 unlock → wire-worker 派发缝。从 binary-private
//! 上移到 lib 后，`tests/daemon_e2e.rs` 可直接调真生产函数（而非复刻接线），main.rs `run()` 改为
//! 调用本模块导出的它们（main.rs 只剩薄入口 + 线程编排）。
//!
//! **搬迁不改逻辑**：行为与上移前 100% 一致；I/O 层（listen18022/listen6672 PF_PACKET、wire
//! sender）零改动，本模块只组装注入缝。

use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::Arc;

use crate::automation_state;
use crate::codec;
use crate::config::Config;
use crate::control::UnlockDispatch;
use crate::daemon::{self, Job, PushTracker, UnlockJob};
use crate::ha_push::HaPushClient;
use crate::listen18022::{self, DetectedFrame};
use crate::listen6672;
use crate::unlock::{UnlockOutcome, WireKind};
use crate::wire_sender;

// ---------------------------------------------------------------------------
// load_automation_flags（§5.1/5.2/5.3，锚 Go loadAutomationFlags）
// ---------------------------------------------------------------------------

/// 读 state 文件字节，解析出两 bool。文件不存在返 `Ok(None)`（降级到 config 不 warn）；
/// 存在但损坏返 `Err`（caller log warning + 降级）。
pub fn load_state_file(path: &str) -> Result<Option<automation_state::State>, String> {
    match std::fs::read(path) {
        Ok(raw) => automation_state::parse(&raw)
            .map(Some)
            .map_err(|_| "state file present but unparseable".to_string()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("read {path}: {e}")),
    }
}

/// 按优先级解析 auto_unlock/auto_hangup 初值并返回来源（锚 Go `loadAutomationFlags`）。
///
/// 优先级：
///   ① `state` 文件存在且可解析出两 bool → 用之（来源 "state"）。
///   ② 否则 config.ini `[automation]` 出厂默认（来源 "config"；section 缺失两 flag 缺省 false）。
///   ③ 否则 env `DOORACCESS_EXPERIMENTAL_AUTO_*`（来源 "env"，退役 fallback）。
///
/// state 存在但空/半写/非法/缺 key → log warning + 整文件丢弃降级到②（禁 crash、禁部分恢复）。
/// env 被②config 压掉时若 env 有设值 log 一行提示（§5.3）。
pub fn load_automation_flags(
    state_path: &str,
    cfg: &Config,
    logf: &mut dyn FnMut(&str),
) -> (bool, bool, &'static str) {
    // ① state 文件
    match load_state_file(state_path) {
        Ok(Some(st)) => return (st.auto_unlock, st.auto_hangup, "state"),
        Ok(None) => {} // 不存在：静默降级
        Err(msg) => {
            // 存在但解析失败 → log warning + 降级，禁 crash（§5.2）。
            logf(&format!(
                "automation.state load failed, falling back to config defaults: {msg}"
            ));
        }
    }

    // ② config.ini [automation]（config.ini 已成功加载即此层级命中）。
    // env 被 config 压掉时提示（§5.3）。
    if std::env::var_os("DOORACCESS_EXPERIMENTAL_AUTO_UNLOCK").is_some()
        || std::env::var_os("DOORACCESS_EXPERIMENTAL_AUTO_HANGUP").is_some()
    {
        logf("DOORACCESS_EXPERIMENTAL_AUTO_* set but ignored (config.ini present; flags from [automation])");
    }
    (cfg.automation.auto_unlock, cfg.automation.auto_hangup, "config")
}

// ---------------------------------------------------------------------------
// build_listen18022 + OnDetect（§3.4，锚 Go buildListen18022/onRingDetected）
// ---------------------------------------------------------------------------

/// 从 `cfg.stations` 构造 IP → 外机 SIP URI 反向映射（纯函数；锚 Go `parseStationsToIPMap`）。
pub fn parse_stations_to_ip_map(cfg: &Config) -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::with_capacity(cfg.stations.len());
    for st in &cfg.stations {
        if let Ok((_, ip_str, _)) = codec::parse_uri(&st.sip) {
            m.insert(ip_str, st.sip.clone());
        }
    }
    m
}

/// 构造 listen18022 Listener 并安装 `OnDetect`（§3.4）。slaves 为空返 `None`（不启动，
/// 与 Go `buildListen18022` 一致）。
///
/// OnDetect 两件事（锚 Go `buildListen18022`/`onRingDetected`）：
///   ① 每帧 `format_log` syslog（含 req/方向/src-dst）。
///   ② req=704 + src∈cfg.Stations + dst==本机室内机 → 门铃 `event=ring` detached push。
///      to(roomURI) 默认 cfg.SIP；dst≠indoorIP 时按 Go 重建 `indoorName@dst_ip:18022`；
///      src 不在 cfg.Stations 则 log dropped 不 push。
///
/// **范围红线**：只做 FormatLog + event=ring push，**绝不**含 filter+debounce→Unlock job
/// 自动产出（那属姊妹 change ②）。
pub fn build_listen18022(
    cfg: &Config,
    slaves: Vec<String>,
    push_client: Arc<HaPushClient>,
    shutdown: Arc<AtomicBool>,
    tracker: PushTracker,
) -> Option<listen18022::Listener> {
    if slaves.is_empty() {
        return None;
    }

    // 室内机名 + IP（cfg.SIP = monitor URI）。
    let (indoor_name, indoor_ip): (String, Option<[u8; 4]>) = match codec::parse_uri(&cfg.sip) {
        Ok((name, ip_str, _)) => (name, parse_ipv4(&ip_str)),
        Err(_) => (String::new(), None),
    };

    // 外机 IP → SIP 反查 + daemon/outdoor IPs（InferDirection 用）。
    let outdoor_by_ip = parse_stations_to_ip_map(cfg);
    let outdoor_ips: Vec<[u8; 4]> = outdoor_by_ip
        .keys()
        .filter_map(|s| parse_ipv4(s))
        .collect();
    // daemon IP：从 iface 读（与 wire_sender::get_iface_ip 同源）。
    let daemon_ip: Option<[u8; 4]> = if cfg.iface.is_empty() {
        None
    } else {
        wire_sender::get_iface_ip(&cfg.iface)
            .ok()
            .map(|v4| v4.octets())
    };

    let cfg_sip = cfg.sip.clone();
    let on_detect: listen18022::OnDetect = Box::new(move |d: &DetectedFrame| {
        // ① 每帧 FormatLog syslog。
        let dir = listen18022::infer_direction(
            d.src_ip,
            d.dst_ip,
            daemon_ip,
            indoor_ip,
            &outdoor_ips,
        );
        eprintln!(
            "dooraccess-rs: {}",
            listen18022::format_log(d.req, d.src_ip, d.dst_ip, &d.body, dir)
        );

        // ② req=704 门铃 push（仅本机外机 src + dst==本机室内机）。
        if d.req != 704 {
            return;
        }
        let src_str = ipv4_str(d.src_ip);
        let outdoor_uri = match outdoor_by_ip.get(&src_str) {
            Some(u) => u.clone(),
            None => {
                eprintln!(
                    "dooraccess-rs: ring: src={src_str} not in cfg.Stations (dropped, neighbor or unconfigured station)"
                );
                return;
            }
        };
        // roomURI 默认 cfg.SIP；dst≠indoorIP（罕见配置错位）→ 重建 indoorName@dst_ip:18022。
        let mut room_uri = cfg_sip.clone();
        if let Some(iip) = indoor_ip {
            if iip != d.dst_ip && !indoor_name.is_empty() {
                room_uri = format!("{indoor_name}@{}:18022", ipv4_str(d.dst_ip));
            }
        }
        // 门铃 push（detached 线程，off wire-worker）。
        daemon::spawn_push(
            &tracker,
            Arc::clone(&push_client),
            Arc::clone(&shutdown),
            "ring".into(),
            vec![
                ("from".into(), outdoor_uri),
                ("to".into(), room_uri),
                ("result".into(), "0".into()),
            ],
        );
    });

    Some(listen18022::Listener::new(slaves, Some(on_detect)))
}

// ---------------------------------------------------------------------------
// UDP 6672 号码查询响应 callback（§8.1/8.2，锚 Go handleNumberQuery main.go:463）
// ---------------------------------------------------------------------------

/// 构造 listen6672 `on_number_query` 回调（§8.1/8.2）。返 `None` 时不安装（cfg.sip 解析失败 →
/// 无本机号码可比对，整体不响应任何号码查询）。
///
/// 回调逻辑（锚 Go `handleNumberQuery`）：
///   ① `DecodeBCD(frame.target_bcd) == myNum`（cfg.SIP 的 monitor 号码）→ 命中；否则安静 drop。
///   ② 本机门禁网 IPv4 解析优先级（Go **代码**分支，main.go:473-485）：
///        `sender.LocalIP`（本骨架 wire_sender::Sender.local_ip=None，故此层不命中）
///        → `GetIfaceIP(cfg.iface)` → drop（注释提的"srcIP 网段推导"代码无此分支，不实现）。
///      本机 IP 在 setup 期解析一次（iface 固定，运行期不变）；解析失败 → 回调内 drop。
///   ③ `build_number_query_response` 构帧 → `send_udp_response` 单播回 srcIP 本机门禁网 IPv4
///      （NBO 由 SocketAddrV4/[u8;4] 原语承载，§8.0 已处理，不手写移位）。
pub fn build_number_query_callback(cfg: &Config) -> Option<listen6672::FrameCallback> {
    // cfg.SIP 的 monitor 号码（本机号码）；解析失败 → 不安装回调。
    let my_num = match codec::parse_uri(&cfg.sip) {
        Ok((name, _, _)) => name,
        Err(_) => {
            eprintln!("dooraccess-rs: number_query: cfg.sip parse failed, callback not installed");
            return None;
        }
    };

    // 本机门禁网 IPv4（Go 代码分支 sender.LocalIP→GetIfaceIP→drop；local_ip=None → 走 GetIfaceIP）。
    // setup 期解析一次（iface 固定）；None → 回调内每次 drop（无本机 IP 可回）。
    let our_ip: Option<[u8; 4]> = if cfg.iface.is_empty() {
        None
    } else {
        wire_sender::get_iface_ip(&cfg.iface).ok().map(|v4| v4.octets())
    };
    let iface = cfg.iface.clone();

    Some(Box::new(move |src_ip: [u8; 4], frame: &listen6672::Frame| {
        // ① 是否查本机号码。
        if codec::decode_bcd(frame.target_bcd) != my_num {
            return; // 不是查我，drop（其它邻居各自响应自己的号码）。
        }
        // ② 本机门禁网 IPv4。
        let Some(ip) = our_ip else {
            eprintln!(
                "dooraccess-rs: number_query: cannot determine local IPv4 (iface={iface:?})"
            );
            return;
        };
        // ③ 构帧 + 单播回 srcIP。
        let resp = listen6672::build_number_query_response(frame, ip);
        match listen6672::send_udp_response(src_ip, &resp) {
            Ok(()) => eprintln!(
                "dooraccess-rs: number_query: replied to {} with our IP {}",
                ipv4_str(src_ip),
                ipv4_str(ip),
            ),
            Err(e) => eprintln!("dooraccess-rs: number_query: send: {e}"),
        }
    }))
}

// ---------------------------------------------------------------------------
// WorkerUnlockDispatch：control.rs /unlock handler → wire-worker 队列（决策 5 / §4.1/4.2）
// ---------------------------------------------------------------------------

/// 手动 unlock 派发缝：HTTP `/unlock` handler 经此投 [`Job::Unlock`] 到单 wire-worker 队列、
/// 阻塞等 worker 经一次性 reply channel 回灌 [`UnlockOutcome`]，再由 handler 做 4 分支 HTTP
/// 映射（决策 5：手动 unlock 经 worker、消解 wireMu）。
///
/// **失活解锁不变量（决策 8）**：
///   - 投 job 容忍 `SendError`（worker 已退则返退化 wire-failure，禁 unwrap）。
///   - 等 reply：worker 正常跑完回灌真 outcome；worker panic → 其持有的 `UnlockJob`（含 reply
///     sender）teardown drop → 本处 `recv()` 收 `RecvError` → 退化 wire-failure（不永等）。
pub struct WorkerUnlockDispatch {
    pub job_tx: mpsc::Sender<Job>,
}

impl UnlockDispatch for WorkerUnlockDispatch {
    fn dispatch(
        &self,
        caller_bcd: [u8; 4],
        callee_bcd: [u8; 4],
        target_ip: &str,
        target_port: u16,
    ) -> UnlockOutcome {
        // 一次性 reply channel（SyncSender(1)；worker 跑完回灌阻塞中的 handler）。
        let (reply_tx, reply_rx) = mpsc::sync_channel::<UnlockOutcome>(1);
        let job = Job::Unlock(UnlockJob {
            caller_bcd,
            callee_bcd,
            target_ip: target_ip.to_string(),
            target_port,
            reply: reply_tx,
        });
        // 投 job：容忍 SendError（worker 已退 → 退化 wire-failure，禁 unwrap，决策 8）。
        if daemon::submit_job(&self.job_tx, job).is_err() {
            return worker_unavailable_outcome();
        }
        // 等 worker 回灌：正常 → 真 outcome；worker panic（reply sender drop）→ RecvError →
        // 退化 wire-failure（不永等，决策 8 worker-death）。
        match reply_rx.recv() {
            Ok(outcome) => outcome,
            Err(_) => worker_unavailable_outcome(),
        }
    }
}

/// worker 不可用（已退 / panic）时的退化 outcome：wire-failure（Other → handler 映射 503/-1）。
pub fn worker_unavailable_outcome() -> UnlockOutcome {
    UnlockOutcome {
        result: codec::result::ERR,
        retries: 0,
        terminated_by_bye: false,
        wire_kind: Some(WireKind::Other),
    }
}

// ---------------------------------------------------------------------------
// 共享小工具（IP 渲染/解析）
// ---------------------------------------------------------------------------

/// 把 `[u8; 4]` 渲染成点分十进制（号码/方向日志用）。
pub fn ipv4_str(ip: [u8; 4]) -> String {
    format!("{}.{}.{}.{}", ip[0], ip[1], ip[2], ip[3])
}

/// 解析点分十进制 IPv4 → `[u8; 4]`（失败返 `None`）。
pub fn parse_ipv4(s: &str) -> Option<[u8; 4]> {
    s.parse::<std::net::Ipv4Addr>().ok().map(|v4| v4.octets())
}
