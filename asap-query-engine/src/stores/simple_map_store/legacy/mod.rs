#![allow(deprecated)]

pub mod global;
pub mod per_key;

pub use global::LegacySimpleMapStoreGlobal;
pub use per_key::LegacySimpleMapStorePerKey;
