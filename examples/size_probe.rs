// size_probe — Phase1 体型测量辅助（非生产）。
//
// 用 black_box 实际调用 codec/wire18022/config/automation_state 四模块的代表函数，
// 阻止 dead-code elimination 剥除 lib，使交叉编译出的 MIPS binary 字节数反映
// 「Phase1 模块真正链入时的体型」（probe binary 因 main 不 use lib 测不出增量）。
// 仅用于 `cargo build --example size_probe`，不进生产路径。
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

    println!("size_probe ok");
}
