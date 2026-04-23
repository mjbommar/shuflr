//! Minimal JSON syntactic validator used by `shuflr verify --deep`.
//!
//! We don't build a tree — we only check that each record is a
//! syntactically valid RFC 8259 JSON *value* with bounded nesting. This
//! is cheap enough to run on every record without blowing the dep
//! graph (we depend on no JSON parser otherwise).
//!
//! Depth cap: `{` and `[` each open a new level; we reject at
//! `depth > max_depth`. Strings have no nesting. Recursion is iterative
//! via an explicit stack, so we can't stack-overflow on pathological
//! inputs.

use std::fmt;

/// What kind of syntax error was found. Enough detail for an error log
/// line without dragging in a full error-location story.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonError {
    /// Ran out of input while in the middle of a value.
    UnexpectedEof { at: usize },
    /// Unexpected byte at the given position.
    Unexpected { at: usize, byte: u8 },
    /// Depth exceeded `max_depth`.
    TooDeep { at: usize, limit: u32 },
    /// Trailing non-whitespace after the top-level value.
    Trailing { at: usize },
    /// A string literal contained a raw control byte (<0x20) or bad escape.
    BadString { at: usize },
    /// A `\u` escape wasn't four hex digits.
    BadUnicode { at: usize },
    /// A number was malformed (e.g. `01`, leading `.`, lone `-`).
    BadNumber { at: usize },
    /// An identifier (`true`/`false`/`null`) didn't match.
    BadLiteral { at: usize },
}

impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonError::UnexpectedEof { at } => write!(f, "unexpected eof at byte {at}"),
            JsonError::Unexpected { at, byte } => {
                write!(f, "unexpected byte {byte:#04x} at byte {at}")
            }
            JsonError::TooDeep { at, limit } => {
                write!(f, "nesting exceeds depth cap {limit} at byte {at}")
            }
            JsonError::Trailing { at } => write!(f, "trailing data after value at byte {at}"),
            JsonError::BadString { at } => write!(f, "malformed string at byte {at}"),
            JsonError::BadUnicode { at } => write!(f, "bad \\u escape at byte {at}"),
            JsonError::BadNumber { at } => write!(f, "malformed number at byte {at}"),
            JsonError::BadLiteral { at } => write!(f, "bad literal at byte {at}"),
        }
    }
}

/// Validate that `input` is a syntactically valid JSON value with
/// `{`/`[` nesting at most `max_depth`. Leading and trailing whitespace
/// is permitted; no non-whitespace may follow the top-level value.
pub fn validate(input: &[u8], max_depth: u32) -> Result<(), JsonError> {
    let mut p = Parser {
        input,
        pos: 0,
        max_depth,
    };
    p.skip_ws();
    p.parse_value()?;
    p.skip_ws();
    if p.pos < p.input.len() {
        return Err(JsonError::Trailing { at: p.pos });
    }
    Ok(())
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
    max_depth: u32,
}

#[derive(Copy, Clone)]
enum Container {
    ObjectAfterKey,
    ObjectAfterValue,
    ArrayAfterValue,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Some(b)
    }

    fn expect(&mut self, byte: u8) -> Result<(), JsonError> {
        match self.bump() {
            Some(b) if b == byte => Ok(()),
            Some(b) => Err(JsonError::Unexpected {
                at: self.pos - 1,
                byte: b,
            }),
            None => Err(JsonError::UnexpectedEof { at: self.pos }),
        }
    }

    fn skip_ws(&mut self) {
        while let Some(b) = self.peek() {
            match b {
                b' ' | b'\t' | b'\n' | b'\r' => self.pos += 1,
                _ => break,
            }
        }
    }

    /// Iterative value parser with an explicit container stack so we
    /// never recurse on user input.
    fn parse_value(&mut self) -> Result<(), JsonError> {
        let mut stack: Vec<Container> = Vec::new();
        'outer: loop {
            self.skip_ws();
            let start = self.pos;
            let b = self
                .peek()
                .ok_or(JsonError::UnexpectedEof { at: self.pos })?;
            match b {
                b'{' => {
                    if stack.len() as u32 >= self.max_depth {
                        return Err(JsonError::TooDeep {
                            at: start,
                            limit: self.max_depth,
                        });
                    }
                    self.pos += 1;
                    self.skip_ws();
                    // Empty object?
                    if self.peek() == Some(b'}') {
                        self.pos += 1;
                        // fall through to close handler below
                    } else {
                        // Parse first key, then ':'.
                        self.parse_string()?;
                        self.skip_ws();
                        self.expect(b':')?;
                        stack.push(Container::ObjectAfterKey);
                        continue 'outer;
                    }
                }
                b'[' => {
                    if stack.len() as u32 >= self.max_depth {
                        return Err(JsonError::TooDeep {
                            at: start,
                            limit: self.max_depth,
                        });
                    }
                    self.pos += 1;
                    self.skip_ws();
                    if self.peek() == Some(b']') {
                        self.pos += 1;
                    } else {
                        stack.push(Container::ArrayAfterValue);
                        continue 'outer;
                    }
                }
                b'"' => self.parse_string()?,
                b't' | b'f' | b'n' => self.parse_literal()?,
                b'-' | b'0'..=b'9' => self.parse_number()?,
                _ => {
                    return Err(JsonError::Unexpected { at: start, byte: b });
                }
            }

            // After emitting a value, walk up the stack to decide what
            // comes next.
            loop {
                let Some(frame) = stack.last().copied() else {
                    return Ok(());
                };
                self.skip_ws();
                let c = self
                    .peek()
                    .ok_or(JsonError::UnexpectedEof { at: self.pos })?;
                match frame {
                    Container::ObjectAfterKey => {
                        // We just parsed the value for a key. Expect , or }.
                        // SAFETY: we just observed `stack.last()` was Some in the guard above.
                        let top = stack.len() - 1;
                        stack[top] = Container::ObjectAfterValue;
                        continue;
                    }
                    Container::ObjectAfterValue => match c {
                        b',' => {
                            self.pos += 1;
                            self.skip_ws();
                            self.parse_string()?;
                            self.skip_ws();
                            self.expect(b':')?;
                            // SAFETY: we just observed `stack.last()` was Some in the guard above.
                            let top = stack.len() - 1;
                            stack[top] = Container::ObjectAfterKey;
                            continue 'outer;
                        }
                        b'}' => {
                            self.pos += 1;
                            stack.pop();
                            continue;
                        }
                        _ => {
                            return Err(JsonError::Unexpected {
                                at: self.pos,
                                byte: c,
                            });
                        }
                    },
                    Container::ArrayAfterValue => match c {
                        b',' => {
                            self.pos += 1;
                            continue 'outer;
                        }
                        b']' => {
                            self.pos += 1;
                            stack.pop();
                            continue;
                        }
                        _ => {
                            return Err(JsonError::Unexpected {
                                at: self.pos,
                                byte: c,
                            });
                        }
                    },
                }
            }
        }
    }

    fn parse_string(&mut self) -> Result<(), JsonError> {
        let start = self.pos;
        self.expect(b'"')
            .map_err(|_| JsonError::BadString { at: start })?;
        loop {
            let b = self
                .bump()
                .ok_or(JsonError::UnexpectedEof { at: self.pos })?;
            match b {
                b'"' => return Ok(()),
                b'\\' => {
                    let esc = self
                        .bump()
                        .ok_or(JsonError::UnexpectedEof { at: self.pos })?;
                    match esc {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {}
                        b'u' => {
                            for _ in 0..4 {
                                let d = self
                                    .bump()
                                    .ok_or(JsonError::UnexpectedEof { at: self.pos })?;
                                if !d.is_ascii_hexdigit() {
                                    return Err(JsonError::BadUnicode { at: self.pos - 1 });
                                }
                            }
                        }
                        _ => return Err(JsonError::BadString { at: self.pos - 1 }),
                    }
                }
                0..=0x1F => {
                    return Err(JsonError::BadString { at: self.pos - 1 });
                }
                _ => {}
            }
        }
    }

    fn parse_literal(&mut self) -> Result<(), JsonError> {
        let start = self.pos;
        let expect: &[u8] = match self.peek() {
            Some(b't') => b"true",
            Some(b'f') => b"false",
            Some(b'n') => b"null",
            _ => return Err(JsonError::BadLiteral { at: start }),
        };
        if self.input.len() < start + expect.len()
            || &self.input[start..start + expect.len()] != expect
        {
            return Err(JsonError::BadLiteral { at: start });
        }
        self.pos += expect.len();
        Ok(())
    }

    fn parse_number(&mut self) -> Result<(), JsonError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        // int part
        match self.peek() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                self.pos += 1;
                while let Some(b'0'..=b'9') = self.peek() {
                    self.pos += 1;
                }
            }
            _ => return Err(JsonError::BadNumber { at: start }),
        }
        // frac
        if self.peek() == Some(b'.') {
            self.pos += 1;
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(JsonError::BadNumber { at: start });
            }
            while let Some(b'0'..=b'9') = self.peek() {
                self.pos += 1;
            }
        }
        // exp
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(JsonError::BadNumber { at: start });
            }
            while let Some(b'0'..=b'9') = self.peek() {
                self.pos += 1;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) {
        validate(s.as_bytes(), 128).unwrap_or_else(|e| panic!("expected ok on {s:?}: {e}"));
    }
    fn err(s: &str) {
        assert!(
            validate(s.as_bytes(), 128).is_err(),
            "expected error on {s:?}"
        );
    }

    #[test]
    fn scalars_and_literals() {
        ok("null");
        ok("true");
        ok("false");
        ok("0");
        ok("-0");
        ok("42");
        ok("-3.14");
        ok("1e10");
        ok("1E-2");
        ok("\"\"");
        ok("\"hello\"");
        ok("\"with \\\"escape\\\"\"");
        ok("\"unicode \\u00FF\"");
    }

    #[test]
    fn rejects_malformed_scalars() {
        err("tru");
        err("truex");
        err("01"); // leading zero disallowed
        err(".5"); // leading dot
        err("-"); // lone minus
        err("1."); // trailing dot
        err("1e"); // dangling exp
        err("\"unterminated");
        err("\"bad \\x escape\"");
        err("\"bad \\u00\""); // short unicode
        err("\"\x01 raw ctrl\"");
    }

    #[test]
    fn objects_and_arrays() {
        ok("{}");
        ok("[]");
        ok("[1,2,3]");
        ok(r#"{"a":1,"b":[true,null,"x"]}"#);
        ok(r#"{ "k" : "v" ,  "n" : 0 }"#);
    }

    #[test]
    fn rejects_malformed_structures() {
        err("{");
        err("}");
        err("[");
        err("[,]");
        err("[1,]");
        err("{\"a\":}");
        err("{\"a\" 1}");
        err("{1:2}"); // keys must be strings
    }

    #[test]
    fn trailing_garbage_is_rejected() {
        err("null extra");
        err("{}x");
        err("[]]");
    }

    #[test]
    fn whitespace_ok_both_sides() {
        ok("  null  ");
        ok("\n\t{}\r\n");
    }

    #[test]
    fn depth_cap_triggers_on_excess_nesting() {
        // Depth cap = 3 → three levels fit, four doesn't.
        validate(b"[[[1]]]", 3).expect("3 deep fits");
        assert!(matches!(
            validate(b"[[[[1]]]]", 3),
            Err(JsonError::TooDeep { .. })
        ));
        // Mix of {} and [] counts against the same cap.
        assert!(matches!(
            validate(br#"{"a":[{"b":1}]}"#, 2),
            Err(JsonError::TooDeep { .. })
        ));
    }

    #[test]
    fn deeply_nested_but_within_cap() {
        let mut s = String::new();
        for _ in 0..100 {
            s.push('[');
        }
        s.push('1');
        for _ in 0..100 {
            s.push(']');
        }
        ok(&s);
    }

    #[test]
    fn no_stack_overflow_on_pathological_input() {
        // 10_000 opens is deeper than any reasonable stack, but we
        // heap-allocate the parser stack so this must only trip the
        // depth cap, not crash.
        let s: String = "[".repeat(10_000);
        assert!(matches!(
            validate(s.as_bytes(), 128),
            Err(JsonError::TooDeep { .. })
        ));
    }

    #[test]
    fn empty_input_is_error() {
        err("");
        err("   ");
    }
}
