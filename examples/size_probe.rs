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
        black_box(&[("result", JsonValue::Number(0)), ("auto_unlock", JsonValue::Bool(true))]),
        JsonOptions::MARSHAL,
    ));
    black_box(&body);

    let mut reader = black_box(&b"{\"on\":true}"[..]);
    let parsed = black_box(dooraccess_rs::control::parse_json_body(&mut reader));
    black_box(&parsed);

    let mux = black_box(dooraccess_rs::httpx::mux::ServeMux::new());
    black_box(&mux);

    let sd = black_box(dooraccess_rs::info::build(black_box(None), black_box("v0.2.0"), false));
    let info_json = black_box(dooraccess_rs::info::render_json(&sd));
    black_box(&info_json);

    let url = black_box(dooraccess_rs::ha_push::build_url(
        black_box("10.0.0.66"),
        black_box(8123),
        black_box("/api/x"),
    ));
    black_box(&url);

    println!("size_probe ok");
}
