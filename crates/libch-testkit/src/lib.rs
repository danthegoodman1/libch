//! What libch's tests, benchmarks and demo share: where the dev stack and the BigHouse checkout are, a tenant
//! that cleans up after itself, and measurements of ClickHouse processes.

pub mod conformance;
pub mod measure;
pub mod preflight;
mod reader;
pub mod schema;
mod stack;
mod tenant;

use tokio::sync::OnceCell;

pub use reader::Reader;
pub use stack::Stack;
pub use tenant::{TenantFixture, apply};

/// The dev stack, once the preflight has passed in this process. Panics with the preflight's message when the
/// environment is not ready, so every test that needs the stack fails saying what to start or build.
pub async fn stack() -> &'static Stack {
    static STACK: OnceCell<Stack> = OnceCell::const_new();
    STACK
        .get_or_init(|| async {
            let stack = Stack::from_env();
            if let Err(error) = preflight::environment(&stack, preflight::BIGHOUSE_REV.trim()).await {
                panic!("libch's tests need the dev stack and a built BigHouse checkout: {error}");
            }
            stack
        })
        .await
}
