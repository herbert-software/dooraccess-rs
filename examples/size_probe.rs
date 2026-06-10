// size_probe — Phase1 体型测量辅助（非生产）。
//
// 用 black_box 实际调用 codec/wire18022/config/automation_state 四模块的代表函数，
// 阻止 dead-code elimination 剥除 lib，使交叉编译出的 MIPS binary 字节数反映
// 「Phase1 模块真正链入时的体型」（probe binary 因 main 不 use lib 测不出增量）。
// 仅用于 `cargo build --example size_probe`，不进生产路径。
//
// 体型探针里把线程/socket-spawn 的骨架入口（run_worker/spawn_push/build_listen18022 等）
// 取**函数指针**经 black_box 防 DCE，故须显式写出其多参签名——这些签名复杂度是测量手段
// 固有的（要精确指向那个函数项），非生产代码可简化的对象，故在本 example 放宽。
#![allow(clippy::type_complexity)]
use std::hint::black_box;

fn main() {
    let bcd = black_box(dooraccess_rs::codec::encode_bcd(black_box("06021103")));
    black_box(&bcd);
    let frame = black_box(dooraccess_rs::wire18022::build_preview_frame());
    black_box(&frame);
    let st = black_box(dooraccess_rs::automation_state::parse(black_box(
        b"auto_unlock=true\nauto_hangup=false\n",
    )));
    black_box(&st);
    let uri_ok = black_box(dooraccess_rs::config::validate_uri(black_box(
        "06021103@10.0.0.10:18022",
    )));
    black_box(&uri_ok);

    // --- Phase2 模块链入（httpx / control / info / ha_push）---
    use dooraccess_rs::httpx::json::{encode_struct, JsonOptions, JsonValue};
    let body = black_box(encode_struct(
        black_box(&[
            ("result", JsonValue::Number(0)),
            ("auto_unlock", JsonValue::Bool(true)),
        ]),
        JsonOptions::MARSHAL,
    ));
    black_box(&body);

    let mut reader = black_box(&b"{\"on\":true}"[..]);
    let parsed = black_box(dooraccess_rs::control::parse_json_body(&mut reader));
    black_box(&parsed);

    let mux = black_box(dooraccess_rs::httpx::mux::ServeMux::new());
    black_box(&mux);

    let sd = black_box(dooraccess_rs::info::build(
        black_box(None),
        black_box("v0.2.0"),
        false,
    ));
    let info_json = black_box(dooraccess_rs::info::render_json(&sd));
    black_box(&info_json);

    let url = black_box(dooraccess_rs::ha_push::build_url(
        black_box("10.0.0.66"),
        black_box(8123),
        black_box("/api/x"),
    ));
    black_box(&url);

    // --- Phase3 模块链入（ffi / bpf / listen6672 / listen18022 / wire_sender / unlock）---
    // 只引代表性纯函数（不开真 socket），black_box 防 DCE，使 MIPS binary 反映 Phase3 体型。

    // ffi: htons + BPF fprog 头构造。
    let net = black_box(dooraccess_rs::ffi::htons(black_box(0x0003)));
    black_box(&net);
    let fprog = black_box(dooraccess_rs::bpf::as_fprog(&dooraccess_rs::bpf::BPF_6672));
    black_box(&fprog);
    let fprog2 = black_box(dooraccess_rs::bpf::as_fprog(&dooraccess_rs::bpf::BPF_18022));
    black_box(&fprog2);

    // listen6672: parser + classify + numquery 响应构造 + L2 extract。
    let frame6672 = black_box(dooraccess_rs::listen6672::parse_frame(black_box(&[
        0x00, 0x06, 0x02, 0x11, 0x03, 0x06, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x40, 0x94, 0xb6,
    ])));
    if let Ok(f) = &frame6672 {
        let kind = black_box(dooraccess_rs::listen6672::classify(f));
        black_box(&kind);
        let resp = black_box(dooraccess_rs::listen6672::build_number_query_response(
            f,
            black_box([10, 0, 0, 1]),
        ));
        black_box(&resp);
    }
    black_box(&frame6672);
    let udp_payload = black_box(dooraccess_rs::listen6672::extract_udp_payload(black_box(
        &[0u8; 64],
    )));
    black_box(&udp_payload);

    // listen18022: frame parser + L2 extract + dedup key。
    let f18022 = black_box(dooraccess_rs::listen18022::parse_frame(black_box(
        b"req=518&query=x",
    )));
    black_box(&f18022);
    let tcp_payload = black_box(dooraccess_rs::listen18022::extract_tcp_payload(black_box(
        &[0u8; 64],
    )));
    black_box(&tcp_payload);
    let dk = black_box(dooraccess_rs::listen18022::dedup_key(
        black_box([10, 0, 0, 1]),
        black_box(18022),
        black_box(b"frame"),
    ));
    black_box(&dk);

    // wire_sender: 错误分类 + 源 IP 拣选（纯函数，不开 socket）。
    let retry = black_box(dooraccess_rs::wire_sender::is_retryable_error(
        &dooraccess_rs::wire_sender::WireError::SilentFin,
    ));
    black_box(&retry);
    let ip = black_box(dooraccess_rs::wire_sender::pick_ipv4_from_addrs(black_box(
        &[std::net::Ipv4Addr::new(10, 0, 0, 2)],
    )));
    black_box(&ip);

    // unlock: wire 错误分类 + bye 订阅 filter 构造（核心逻辑入口；retry 主体经 trait 注入）。
    let code = black_box(dooraccess_rs::unlock::classify_wire_err(
        dooraccess_rs::unlock::WireKind::SilentFin,
    ));
    black_box(&code);
    let bye_filter = black_box(dooraccess_rs::unlock::filter_req708_for_target(black_box(
        Some([10, 0, 0, 1]),
    )));
    black_box(&bye_filter);

    // --- Phase4 骨架模块链入（daemon worker/job/push + orchestration + Persister）---
    // 本 change 新增/移植的代表函数。线程/socket-spawn 的入口（run_worker/spawn_push 等）
    // 取函数指针经 black_box 防 DCE（不在体型探针里真起线程）；纯函数直接调用。

    // daemon: PushTracker 运行期回收 + 取消/排空骨架函数指针。
    let tracker = black_box(dooraccess_rs::daemon::PushTracker::new());
    black_box(tracker.len());
    black_box(tracker.is_empty());
    tracker.join_all();
    black_box(&tracker);
    let run_worker_fp: fn(
        dooraccess_rs::daemon::WorkerDeps,
        std::sync::mpsc::Receiver<dooraccess_rs::daemon::Job>,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) = dooraccess_rs::daemon::run_worker;
    black_box(run_worker_fp as usize);
    let spawn_push_fp: fn(
        &dooraccess_rs::daemon::PushTracker,
        std::sync::Arc<dooraccess_rs::ha_push::HaPushClient>,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        String,
        Vec<(String, String)>,
    ) = dooraccess_rs::daemon::spawn_push;
    black_box(spawn_push_fp as usize);
    let submit_job_fp: fn(
        &std::sync::mpsc::Sender<dooraccess_rs::daemon::Job>,
        dooraccess_rs::daemon::Job,
    ) -> Result<(), &'static str> = dooraccess_rs::daemon::submit_job;
    black_box(submit_job_fp as usize);

    // orchestration: load_automation_flags（三级优先级）+ number-query callback + listen18022 builder。
    let cfg = black_box(dooraccess_rs::config::Config::default());
    let mut sink = |_s: &str| {};
    let flags = black_box(dooraccess_rs::orchestration::load_automation_flags(
        black_box("/nonexistent/automation.state"),
        &cfg,
        &mut sink,
    ));
    black_box(&flags);
    let nq_cb = black_box(dooraccess_rs::orchestration::build_number_query_callback(
        &cfg,
    ));
    black_box(nq_cb.is_some());
    let unavail = black_box(dooraccess_rs::orchestration::worker_unavailable_outcome());
    black_box(&unavail);
    black_box(dooraccess_rs::orchestration::ipv4_str(black_box([
        10, 0, 0, 1,
    ])));
    black_box(dooraccess_rs::orchestration::parse_ipv4(black_box(
        "10.0.0.1",
    )));
    let build18022_fp: fn(
        &dooraccess_rs::config::Config,
        Vec<String>,
        std::sync::Arc<dooraccess_rs::ha_push::HaPushClient>,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        dooraccess_rs::daemon::PushTracker,
    ) -> Option<dooraccess_rs::listen18022::Listener> =
        dooraccess_rs::orchestration::build_listen18022;
    black_box(build18022_fp as usize);

    // automation_state Persister: 原子写 + 纯值去重（取构造/persist/write_atomic 防 DCE）。
    let persister = black_box(dooraccess_rs::automation_state::Persister::new(
        std::path::PathBuf::from("/nonexistent/automation.state"),
        Box::new(|| (true, false)),
        None,
    ));
    persister.persist();
    black_box(&persister);
    let write_atomic_fp: fn(
        &std::path::Path,
        dooraccess_rs::automation_state::State,
    ) -> std::io::Result<()> = dooraccess_rs::automation_state::write_atomic;
    black_box(write_atomic_fp as usize);

    // --- Phase4 ② self-unlock 模块链入（port-rust-self-unlock-consumer 组 D / task 6.2）---
    // 链入本 change 新增/移植的代表函数，使 MIPS binary 反映 ② 真正链入时的体型增量。
    // 纯函数（build_stop_frame）直接调用；线程/socket-spawn 入口（spawn_consumer）取函数指针。

    // wire18022::build_stop_frame —— ② 唯一新增 wire-encode 代码（preview-stop req=708 帧，
    // 移植 Go video.BuildStopFrame）。须确实链入体型基线（其字节正确性另由 golden 单测守）。
    let stop = black_box(dooraccess_rs::wire18022::build_stop_frame(
        black_box([0x06, 0x02, 0x00, 0x00]),
        black_box([0x06, 0x02, 0x11, 0x03]),
    ));
    black_box(&stop);

    // self_unlock::spawn_consumer —— ring 消费者线程入口（filter/debounce/产 Job 主体经此链入）。
    // 取单态化函数指针（concrete logf 闭包类型）经 black_box 防 DCE，不真起线程。
    let spawn_consumer_fp: fn(
        &dyn dooraccess_rs::listen18022::Subscribable,
        dooraccess_rs::self_unlock::SelfUnlockDeps,
        fn(&str),
    ) -> Option<std::thread::JoinHandle<()>> = dooraccess_rs::self_unlock::spawn_consumer;
    black_box(spawn_consumer_fp as usize);

    // daemon::HangupJob 构造 + 生产 HangupWire（worker Job::Hangup arm 经 wire_sender 发 req=708）。
    let hangup_job = black_box(dooraccess_rs::daemon::Job::Hangup(
        dooraccess_rs::daemon::HangupJob {
            outdoor_bcd: black_box([0x06, 0x02, 0x00, 0x00]),
            monitor_bcd: black_box([0x06, 0x02, 0x11, 0x03]),
            outdoor_ip: black_box(String::from("10.0.0.1")),
            outdoor_port: black_box(18022),
        },
    ));
    black_box(&hangup_job);

    // --- Phase5 video 模块链入（port-rust-video-forward / task 7.3）---
    // 链入 video 全栈代表函数，使 MIPS binary 反映 video 真正链入时的体型增量。
    // 纯函数（wire start 帧 / FLV 静态构造 / RTCP 构造 / RTP+codec 解析）直接调用；
    // 线程/socket-spawn 入口（RtpReceiver::run / RtcpSender::run / PreviewClient /
    // Manager::start）取函数指针经 black_box 防 DCE，不在体型探针里真起线程/开 socket。

    // wire18022::build_start_frame —— video 信令 req=704 start 帧（与 build_stop_frame 同居）。
    let start_frame = black_box(dooraccess_rs::wire18022::build_start_frame(
        black_box([0x06, 0x02, 0x00, 0x00]),
        black_box([0x06, 0x02, 0x11, 0x03]),
    ));
    black_box(&start_frame);

    // transmux：FLV 静态构造（header / seq-header tag / NALU tag）。
    let flv_hdr = black_box(dooraccess_rs::video::transmux::flv_header());
    black_box(&flv_hdr);
    let avc_cfg = black_box(dooraccess_rs::video::transmux::pack_avc_decoder_config(
        black_box(&[0x67, 0x42, 0x00, 0x1f]),
        black_box(&[0x68, 0xce]),
    ));
    black_box(&avc_cfg);
    let seq_tag = black_box(
        dooraccess_rs::video::transmux::build_avc_sequence_header_tag(
            black_box(&[0x67, 0x42, 0x00, 0x1f]),
            black_box(&[0x68, 0xce]),
            black_box(0),
        ),
    );
    black_box(&seq_tag);

    // rtp：RTP 头解析 + Annex-B NAL 提取。
    let rtp_pkt = black_box(dooraccess_rs::video::rtp::parse_rtp(black_box(&[
        0x80, 0x62, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x01,
    ])));
    black_box(&rtp_pkt);
    let nals = black_box(dooraccess_rs::video::rtp::extract_nals_annexb(
        black_box(&[0x00, 0x00, 0x00, 0x01, 0x67, 0x42]),
        black_box(0),
    ));
    black_box(&nals);
    let rtp_recv_fp: fn(&str) -> std::io::Result<std::net::UdpSocket> =
        dooraccess_rs::video::rtp::bind_udp;
    black_box(rtp_recv_fp as usize);

    // reassembler：分片重组器构造（主体经 push 链入；构造取防 DCE）。
    let reasm = black_box(dooraccess_rs::video::reassembler::FrameReassembler::new(
        None,
    ));
    black_box(&reasm);

    // frame_buffer：SPS/PPS/IDR 种子缓存 + fan-out 订阅。
    let fb = black_box(dooraccess_rs::video::frame_buffer::FrameBuffer::new());
    black_box(&fb);

    // rtcp：RR/SDES/BYE 复合包构造（CNAME 字面，golden 守字节）。
    let rr_sdes = black_box(dooraccess_rs::video::rtcp::build_rr_sdes(
        black_box(0x1234_5678),
        black_box(0x9abc_def0),
    ));
    black_box(&rr_sdes);
    let bye = black_box(dooraccess_rs::video::rtcp::build_bye(black_box(
        0x1234_5678,
    )));
    black_box(&bye);

    // session：outdoor/caller 解析（codec 复用）。
    let outdoor = black_box(dooraccess_rs::video::session::parse_outdoor(black_box(
        "06020000@10.0.0.10:18022",
    )));
    black_box(&outdoor);
    let caller = black_box(dooraccess_rs::video::session::parse_caller(black_box(
        "06021103",
    )));
    black_box(&caller);

    println!("size_probe ok");
}
