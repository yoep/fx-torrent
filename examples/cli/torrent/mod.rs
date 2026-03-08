pub use info::*;

mod data;
mod info;
mod widgets;

/// The result action for interactive widgets.
#[derive(Debug)]
pub enum ActionResult<T> {
    Ok(T),
    Cancel,
}
