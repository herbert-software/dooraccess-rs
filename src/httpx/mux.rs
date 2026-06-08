//! 极简 path-only 路由（复刻 Go `mux.go`）。

use super::{Handler, Request, ResponseWriter, STATUS_NOT_FOUND, Url, METHOD_GET};

/// path-only 路由表：精确优先，then 最长前缀匹配。
pub struct ServeMux {
    exact: std::collections::HashMap<String, Box<dyn Handler>>,
    prefix: Vec<PrefixEntry>,
}

struct PrefixEntry {
    pattern: String,
    handler: Box<dyn Handler>,
}

impl ServeMux {
    pub fn new() -> Self {
        Self {
            exact: std::collections::HashMap::new(),
            prefix: Vec::new(),
        }
    }

    /// 注册 pattern → handler。尾 `/` 走前缀匹配，否则精确匹配。
    pub fn handle(&mut self, pattern: &str, handler: Option<Box<dyn Handler>>) {
        if pattern.is_empty() {
            panic!("httpx: empty pattern");
        }
        let handler = handler.unwrap_or_else(|| panic!("httpx: nil handler"));

        if pattern.ends_with('/') {
            for e in &self.prefix {
                if e.pattern == pattern {
                    panic!("httpx: duplicate pattern: {pattern}");
                }
            }
            self.prefix.push(PrefixEntry {
                pattern: pattern.to_string(),
                handler,
            });
            // 按长度倒序（长 prefix 优先）
            let last = self.prefix.len() - 1;
            let mut i = last;
            while i > 0 && self.prefix[i].pattern.len() > self.prefix[i - 1].pattern.len() {
                self.prefix.swap(i, i - 1);
                i -= 1;
            }
        } else {
            if self.exact.contains_key(pattern) {
                panic!("httpx: duplicate pattern: {pattern}");
            }
            self.exact.insert(pattern.to_string(), handler);
        }
    }

    pub fn handle_func<F>(&mut self, pattern: &str, f: F)
    where
        F: Fn(&mut dyn ResponseWriter, &Request) + Send + Sync + 'static,
    {
        self.handle(pattern, Some(Box::new(super::HandlerFunc(f))));
    }

    pub fn serve_http(&self, w: &mut dyn ResponseWriter, r: &Request) {
        let path = &r.url.path;
        if let Some(h) = self.match_handler(path) {
            h.serve_http(w, r);
            return;
        }
        http404(w);
    }

    fn match_handler(&self, path: &str) -> Option<&dyn Handler> {
        if let Some(h) = self.exact.get(path) {
            return Some(h.as_ref());
        }
        for e in &self.prefix {
            if path.starts_with(&e.pattern) {
                return Some(e.handler.as_ref());
            }
        }
        None
    }
}

impl Default for ServeMux {
    fn default() -> Self {
        Self::new()
    }
}

fn http404(w: &mut dyn ResponseWriter) {
    w.header()
        .set("Content-Type", "text/plain; charset=utf-8");
    w.header().set("Content-Length", "9");
    w.write_header(STATUS_NOT_FOUND);
    let _ = w.write(b"Not Found");
}

/// 便捷构造测试用 `Request`（仅 path）。
pub fn request_with_path(path: &str) -> Request {
    Request {
        method: METHOD_GET.to_string(),
        url: Url {
            path: path.to_string(),
            raw_query: String::new(),
        },
        header: super::Header::new(),
        body: Vec::new(),
        host: String::new(),
        remote_addr: String::new(),
        content_length: 0,
    }
}
