//! Keys for tenants' operations: minted with a TTL, cached, and revoked on demand.
//!
//! An operation runs under a key rooted at its tenant's directory, so the worker that holds it reaches that
//! tenant alone. Minting a key costs the store a commit, so a [`KeyCache`] mints one the first time a tenant is
//! served and hands the same key out until it nears its expiry.
//!
//! A store holds at most 100,000 keys from every minter together. The cache counts the keys it holds there and
//! keeps the count at or under [`KeyPolicy::capacity`]: when a mint would pass it, the cache revokes the key of
//! the tenant served longest ago. A key some operation still holds stays.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use futures_util::future::join_all;
use inodedb::{Client, Grant};
use tokio::sync::Mutex as AsyncMutex;
use tonic::Code;

use crate::tenant::{TenantKey, store_url};
use crate::{Error, Tenant};

/// How many revocations [`KeyCache::revoke_all`] sends at a time.
const REVOKE_BATCH: usize = 32;

/// How a cache mints and keeps keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyPolicy {
    /// What a key may do. Operations need [`Grant::Write`].
    pub grant: Grant,
    /// How long a key lasts in the store.
    pub ttl: Duration,
    /// A cached key serves a call while it has at least this long left. An operation holds its key from start
    /// to end, so this is at least the longest operation's deadline.
    pub min_remaining: Duration,
    /// The most keys the cache holds in the store at once.
    pub capacity: usize,
}

impl Default for KeyPolicy {
    fn default() -> KeyPolicy {
        KeyPolicy { grant: Grant::Write, ttl: Duration::from_secs(3_600), min_remaining: Duration::from_secs(900), capacity: 10_000 }
    }
}

/// What a cache has done, and what it holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeyStats {
    /// Keys the cache holds in the store now: minted, and neither expired nor revoked.
    pub live: usize,
    /// Keys minted since the cache was made.
    pub minted: u64,
    /// Keys revoked since the cache was made, on demand and to make room.
    pub revoked: u64,
}

/// Mints, caches and revokes the keys of one store's tenants.
///
/// Clones share one cache.
#[derive(Clone)]
pub struct KeyCache {
    inner: Arc<Inner>,
}

struct Inner {
    minter: Client,
    /// The minter's store, as [`store_url`] spells it.
    store: String,
    policy: KeyPolicy,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    slots: HashMap<String, Slotted>,
    /// The tenants with a slot, by when each last asked for its key.
    by_use: BTreeMap<u64, String>,
    clock: u64,
    /// Every key the store may still hold, by mint order, which is expiry order.
    alive: BTreeMap<u64, Alive>,
    /// Mints under way. Each has its place counted before it starts.
    reserved: usize,
    next_serial: u64,
    minted: u64,
    revoked: u64,
}

struct Slotted {
    slot: Arc<Slot>,
    used: u64,
}

struct Alive {
    expires: Instant,
    root: String,
}

/// One tenant's keys. The lock serializes the tenant's mints, so callers that arrive together share one key.
#[derive(Default)]
struct Slot {
    keys: AsyncMutex<Keys>,
}

#[derive(Default)]
struct Keys {
    /// The key new calls get.
    current: Option<Minted>,
    /// Keys the cache stopped handing out before they expired. Operations that started earlier may hold them.
    retired: Vec<Minted>,
    /// Set once the cache has let go of the slot. A call that was waiting for it starts over with a new one.
    dropped: bool,
}

struct Minted {
    serial: u64,
    key: TenantKey,
    expires: Instant,
}

impl Keys {
    fn forget_expired(&mut self, now: Instant) {
        if self.current.as_ref().is_some_and(|key| key.expires <= now) {
            self.current = None;
        }
        self.retired.retain(|key| key.expires > now);
    }

    fn is_empty(&self) -> bool {
        self.current.is_none() && self.retired.is_empty()
    }

    /// Whether no operation holds any of the keys.
    fn is_idle(&self) -> bool {
        self.current.iter().chain(&self.retired).all(|key| key.key.handles() == 1)
    }

    fn take_all(&mut self) -> Vec<Minted> {
        self.current.take().into_iter().chain(self.retired.drain(..)).collect()
    }
}

impl KeyCache {
    /// A cache for the tenants of `minter`'s store. `minter` holds an `admin` key rooted at or above every
    /// tenant it will serve, and names each tenant's directory as [`Tenant::root`].
    pub fn new(minter: Client, policy: KeyPolicy) -> Result<KeyCache, Error> {
        if policy.capacity == 0 {
            return Err(Error::Other("a key cache needs room for at least one key".into()));
        }
        if policy.ttl <= policy.min_remaining {
            return Err(Error::Other(format!(
                "keys that last {:?} never have {:?} left: the TTL must exceed the time a key needs to have left",
                policy.ttl, policy.min_remaining
            )));
        }
        let store = normalize(&minter.endpoint());
        Ok(KeyCache { inner: Arc::new(Inner { minter, store, policy, state: Mutex::new(State::default()) }) })
    }

    pub fn policy(&self) -> &KeyPolicy {
        &self.inner.policy
    }

    /// The tenant's key, minted now when the cache holds none with [`KeyPolicy::min_remaining`] left.
    ///
    /// The cache makes room by revoking keys no handle refers to, so hold the returned handle for as long as
    /// the operation runs.
    pub async fn key(&self, tenant: &Tenant) -> Result<TenantKey, Error> {
        let inner = &self.inner;
        if normalize(&tenant.store) != inner.store {
            return Err(Error::Other(format!("the tenant at `{}` lives in the store {}, and this cache mints for {}", tenant.root, tenant.store, inner.store)));
        }
        loop {
            let slot = inner.slot(&tenant.root);
            let mut keys = slot.keys.lock().await;
            if keys.dropped {
                continue;
            }
            let now = Instant::now();
            if let Some(current) = &keys.current
                && current.expires.saturating_duration_since(now) >= inner.policy.min_remaining
            {
                return Ok(current.key.clone());
            }

            keys.forget_expired(now);
            if let Err(error) = inner.reserve().await {
                inner.release_if_empty(&tenant.root, &mut keys);
                return Err(error);
            }
            // The store counts the TTL from its commit, which comes after this instant, so the cache's idea of
            // the expiry is the earlier of the two.
            let expires = Instant::now() + inner.policy.ttl;
            let minted = inner.minter.mint_key(tenant.root.as_bytes(), inner.policy.grant, inner.policy.ttl).await;
            // A mint whose answer was lost may hold a place in the store until it expires.
            let holds_a_place = minted.as_ref().map_or_else(may_have_minted, |_| true);
            let serial = {
                let mut state = inner.state();
                state.reserved -= 1;
                state.minted += u64::from(minted.is_ok());
                holds_a_place.then(|| {
                    let serial = state.next_serial;
                    state.next_serial += 1;
                    state.alive.insert(serial, Alive { expires, root: tenant.root.clone() });
                    serial
                })
            };
            let key = match minted {
                Ok(key) => TenantKey::from(key),
                Err(error) => {
                    inner.release_if_empty(&tenant.root, &mut keys);
                    return Err(error.into());
                }
            };
            let serial = serial.expect("a minted key has its place counted");
            let replaced = keys.current.replace(Minted { serial, key: key.clone(), expires });
            keys.retired.extend(replaced);
            return Ok(key);
        }
    }

    /// Revokes the tenant's keys in the store, the ones operations hold among them. The tenant's next
    /// [`key`](KeyCache::key) mints anew. Call it as well when the store refuses a cached key: removing a
    /// tenant's directory revokes the keys rooted there, and the cache learns of it here.
    ///
    /// After an error the cache hands none of the keys out again, and a second call revokes what the first left.
    pub async fn revoke(&self, tenant: &Tenant) -> Result<(), Error> {
        self.inner.revoke_root(&tenant.root).await
    }

    /// Revokes every key the cache holds. Returns the first error, after trying each tenant.
    pub async fn revoke_all(&self) -> Result<(), Error> {
        let roots: Vec<String> = self.inner.state().slots.keys().cloned().collect();
        let mut first_error = None;
        for batch in roots.chunks(REVOKE_BATCH) {
            for result in join_all(batch.iter().map(|root| self.inner.revoke_root(root))).await {
                first_error = first_error.or(result.err());
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub fn stats(&self) -> KeyStats {
        let mut state = self.inner.state();
        state.forget_expired(Instant::now());
        KeyStats { live: state.alive.len() + state.reserved, minted: state.minted, revoked: state.revoked }
    }
}

impl std::fmt::Debug for KeyCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyCache").field("store", &self.inner.store).field("policy", &self.inner.policy).field("stats", &self.stats()).finish()
    }
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("no panics holding the cache's state")
    }

    /// The tenant's slot, made when absent, and marked as the one used last.
    fn slot(&self, root: &str) -> Arc<Slot> {
        let mut state = self.state();
        state.clock += 1;
        let now = state.clock;
        let slot = match state.slots.get_mut(root) {
            Some(slotted) => {
                let before = std::mem::replace(&mut slotted.used, now);
                let slot = slotted.slot.clone();
                state.by_use.remove(&before);
                slot
            }
            None => {
                let slot = Arc::new(Slot::default());
                state.slots.insert(root.to_string(), Slotted { slot: slot.clone(), used: now });
                slot
            }
        };
        state.by_use.insert(now, root.to_string());
        slot
    }

    /// Lets go of a slot that holds no key, so a tenant whose mint failed leaves nothing behind.
    fn release_if_empty(&self, root: &str, keys: &mut Keys) {
        if keys.is_empty() {
            keys.dropped = true;
            self.state().remove_slot(root);
        }
    }

    /// Counts a place for one more key, revoking the keys of tenants served longest ago until there is one.
    async fn reserve(&self) -> Result<(), Error> {
        loop {
            let victim = {
                let mut state = self.state();
                state.forget_expired(Instant::now());
                if state.alive.len() + state.reserved < self.policy.capacity {
                    state.reserved += 1;
                    return Ok(());
                }
                state.evict(Instant::now())
            };
            let Some(mut doomed) = victim else {
                return Err(Error::Other(format!("operations hold every one of the cache's {} keys", self.policy.capacity)));
            };
            // A key whose revocation fails keeps its place in the count until it expires.
            while let Some(key) = doomed.pop() {
                self.revoke_key(&key).await?;
            }
        }
    }

    async fn revoke_root(&self, root: &str) -> Result<(), Error> {
        let Some(slot) = self.state().slots.get(root).map(|slotted| slotted.slot.clone()) else { return Ok(()) };
        let mut keys = slot.keys.lock().await;
        if keys.dropped {
            return Ok(());
        }
        keys.forget_expired(Instant::now());
        let mut doomed = keys.take_all();
        while let Some(key) = doomed.pop() {
            if let Err(error) = self.revoke_key(&key).await {
                keys.retired.push(key);
                keys.retired.append(&mut doomed);
                return Err(error);
            }
        }
        keys.dropped = true;
        self.state().remove_slot(root);
        Ok(())
    }

    async fn revoke_key(&self, key: &Minted) -> Result<(), Error> {
        match self.minter.revoke_key(key.key.as_str()).await {
            Ok(()) => {}
            // The store deleted the key when it expired, a moment before the cache's clock said so.
            Err(inodedb::Error::Status(status)) if status.code() == Code::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut state = self.state();
        if state.alive.remove(&key.serial).is_some() {
            state.revoked += 1;
        }
        Ok(())
    }
}

impl State {
    fn remove_slot(&mut self, root: &str) {
        if let Some(slotted) = self.slots.remove(root) {
            self.by_use.remove(&slotted.used);
        }
    }

    /// Stops counting the keys that have expired, and lets go of the slots they leave empty.
    fn forget_expired(&mut self, now: Instant) {
        while let Some(entry) = self.alive.first_entry() {
            if entry.get().expires > now {
                break;
            }
            let root = entry.remove().root;
            // A slot some call holds locked is that call's to settle.
            let Some(slotted) = self.slots.get(&root) else { continue };
            let Ok(mut keys) = slotted.slot.keys.try_lock() else { continue };
            keys.forget_expired(now);
            if keys.is_empty() {
                keys.dropped = true;
                drop(keys);
                self.remove_slot(&root);
            }
        }
    }

    /// Lets go of the slot used longest ago whose keys no operation holds, and returns those keys for
    /// revocation. `None` when operations hold every key.
    fn evict(&mut self, now: Instant) -> Option<Vec<Minted>> {
        let mut emptied = Vec::new();
        let mut victim = None;
        for root in self.by_use.values() {
            let Ok(mut keys) = self.slots[root].slot.keys.try_lock() else { continue };
            keys.forget_expired(now);
            if keys.is_empty() {
                keys.dropped = true;
                emptied.push(root.clone());
            } else if keys.is_idle() {
                keys.dropped = true;
                victim = Some((root.clone(), keys.take_all()));
                break;
            }
        }
        for root in emptied {
            self.remove_slot(&root);
        }
        let (root, doomed) = victim?;
        self.remove_slot(&root);
        Some(doomed)
    }
}

fn normalize(store: &str) -> String {
    store_url(store).trim_end_matches('/').to_string()
}

/// Whether a failed mint may have committed: the store answered nothing the client could read.
fn may_have_minted(error: &inodedb::Error) -> bool {
    matches!(error, inodedb::Error::Status(status) if matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::Unknown | Code::Internal))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minted(serial: u64, expires: Instant) -> Minted {
        Minted { serial, key: TenantKey::from(format!("{serial}.secret")), expires }
    }

    fn state_with(slots: Vec<(&str, Keys)>) -> State {
        let mut state = State::default();
        for (root, keys) in slots {
            state.clock += 1;
            for key in keys.current.iter().chain(&keys.retired) {
                state.alive.insert(key.serial, Alive { expires: key.expires, root: root.to_string() });
            }
            let slot = Arc::new(Slot { keys: AsyncMutex::new(keys) });
            state.slots.insert(root.to_string(), Slotted { slot, used: state.clock });
            state.by_use.insert(state.clock, root.to_string());
        }
        state
    }

    #[test]
    fn eviction_takes_the_idle_slot_used_longest_ago() {
        let now = Instant::now();
        let later = now + Duration::from_secs(60);
        let held = minted(1, later);
        let handle = held.key.clone();
        let mut state = state_with(vec![
            ("held", Keys { current: Some(held), ..Keys::default() }),
            ("expired", Keys { current: Some(minted(2, now)), ..Keys::default() }),
            ("idle", Keys { current: Some(minted(3, later)), retired: vec![minted(4, later)], ..Keys::default() }),
            ("newest", Keys { current: Some(minted(5, later)), ..Keys::default() }),
        ]);

        let doomed = state.evict(now).expect("an idle slot");
        let mut serials: Vec<u64> = doomed.iter().map(|key| key.serial).collect();
        serials.sort_unstable();
        assert_eq!(serials, [3, 4], "the idle tenant's keys, current and retired");
        let mut left: Vec<&str> = state.slots.keys().map(String::as_str).collect();
        left.sort_unstable();
        assert_eq!(left, ["held", "newest"], "the expired slot goes with the scan");
        assert_eq!(state.by_use.len(), 2);

        assert_eq!(state.evict(now).map(|doomed| doomed[0].serial), Some(5));
        assert!(state.evict(now).is_none(), "a held key stays");
        drop(handle);
        assert_eq!(state.evict(now).map(|doomed| doomed[0].serial), Some(1));
        assert!(state.slots.is_empty() && state.by_use.is_empty());
    }

    #[test]
    fn expired_keys_leave_the_count_and_their_slots() {
        let now = Instant::now();
        let soon = now + Duration::from_secs(1);
        let later = now + Duration::from_secs(60);
        let mut state = state_with(vec![
            ("gone", Keys { current: Some(minted(1, soon)), ..Keys::default() }),
            ("renewed", Keys { current: Some(minted(3, later)), retired: vec![minted(2, soon)], ..Keys::default() }),
        ]);
        // A mint whose answer was lost has a place and no slot.
        state.alive.insert(0, Alive { expires: soon, root: "lost".into() });

        state.forget_expired(now);
        assert_eq!(state.alive.len(), 4, "nothing has expired yet");

        state.forget_expired(soon);
        assert_eq!(state.alive.keys().copied().collect::<Vec<_>>(), [3]);
        assert_eq!(state.slots.keys().collect::<Vec<_>>(), ["renewed"]);
        let keys = state.slots["renewed"].slot.keys.try_lock().unwrap();
        assert!(keys.retired.is_empty() && keys.current.as_ref().is_some_and(|key| key.serial == 3));
    }

    #[test]
    fn a_lost_answer_may_have_minted() {
        let status = |code| inodedb::Error::Status(tonic::Status::new(code, "x"));
        assert!(may_have_minted(&status(Code::Unavailable)));
        assert!(may_have_minted(&status(Code::DeadlineExceeded)));
        assert!(!may_have_minted(&status(Code::NotFound)), "the root is absent");
        assert!(!may_have_minted(&status(Code::PermissionDenied)));
        assert!(!may_have_minted(&status(Code::ResourceExhausted)));
        assert!(!may_have_minted(&inodedb::Error::LeaseExpired));
    }

    #[test]
    fn stores_compare_by_address() {
        assert_eq!(normalize("127.0.0.1:7170"), normalize("http://127.0.0.1:7170/"));
        assert_ne!(normalize("127.0.0.1:7170"), normalize("127.0.0.1:7171"));
    }
}
