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
    println!("size_probe ok");
}
