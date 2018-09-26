// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! The macros crate contains all useful needed macros.

/// box try will box error first, and then do the same thing as try!.
#[macro_export]
macro_rules! box_try {
    ($expr:expr) => {{
        match $expr {
            Ok(r) => r,
            Err(e) => return Err(box_err!(e)),
        }
    }};
}

/// A shortcut to box an error.
#[macro_export]
macro_rules! box_err {
    ($e:expr) => ({
        use std::error::Error;
        let e: Box<Error + Sync + Send> = format!("[{}:{}]: {}", file!(), line!(),  $e).into();
        e.into()
    });
    ($f:tt, $($arg:expr),+) => ({
        box_err!(format!($f, $($arg),+))
    });
}

/// make a thread name with additional tag inheriting from current thread.
#[macro_export]
macro_rules! thd_name {
    ($name:expr) => {{
        $crate::util::get_tag_from_thread_name()
            .map(|tag| format!("{}::{}", $name, tag))
            .unwrap_or_else(|| $name.to_owned())
    }};
}

/// Simulating go's defer.
///
/// Please note that, different from go, this defer is bound to scope.
/// When exiting the scope, its deferred calls are executed in last-in-first-out order.
#[macro_export]
macro_rules! defer {
    ($t:expr) => {
        let __ctx = $crate::util::DeferContext::new(|| $t);
    };
}

/// `wait_op!` waits for async operation. It returns `Option<Res>`
/// after the expression get executed.
/// It only accepts an `Result` expression.
#[macro_export]
macro_rules! wait_op {
    ($expr:expr) => {
        wait_op!(IMPL $expr, None)
    };
    ($expr:expr, $timeout:expr) => {
        wait_op!(IMPL $expr, Some($timeout))
    };
    (IMPL $expr:expr, $timeout:expr) => {{
        use std::sync::mpsc;
        let (tx, rx) = mpsc::channel();
        let cb = box move |res| {
            // we don't care error actually.
            let _ = tx.send(res);
        };
        $expr(cb)?;
        match $timeout {
            None => rx.recv().ok(),
            Some(timeout) => rx.recv_timeout(timeout).ok(),
        }
    }};
}

/// `try_opt` check `Result<Option<T>>`, return early when met `Err` or `Ok(None)`.
#[macro_export]
macro_rules! try_opt {
    ($expr:expr) => {{
        match $expr {
            Err(e) => return Err(e.into()),
            Ok(None) => return Ok(None),
            Ok(Some(res)) => res,
        }
    }};
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    #[test]
    fn test_box_error() {
        let file_name = file!();
        let line_number = line!();
        let e: Box<Error + Send + Sync> = box_err!("{}", "hi");
        assert_eq!(
            format!("{}", e),
            format!("[{}:{}]: hi", file_name, line_number + 1)
        );
    }
}
