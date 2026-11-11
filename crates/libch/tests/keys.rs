//! The key cache, against the dev stack's InodeDB store.

use std::time::Duration;

use futures_util::future::join_all;
use libch::inodedb::{Grant, Op};
use libch::keys::{KeyCache, KeyPolicy, KeyStats};
use libch::{Tenant, TenantKey};
use libch_testkit::{TenantFixture, apply, stack};

async fn cache(policy: KeyPolicy) -> KeyCache {
    KeyCache::new(stack().await.admin().await.unwrap(), policy).unwrap()
}

async fn tenants(count: usize) -> Vec<TenantFixture> {
    let stack = stack().await;
    join_all((0..count).map(|_| TenantFixture::create(stack))).await.into_iter().map(Result::unwrap).collect()
}

/// Whether the store lets `key` write in the tenant's directory.
async fn writes(tenant: &Tenant, key: &str) -> bool {
    let Ok(client) = tenant.connect(key).await else { return false };
    let name = format!("probe-{:08x}", rand::random::<u32>());
    apply(&client, [Op::mkdir(name.as_bytes())]).await.is_ok()
}

#[tokio::test]
async fn a_tenant_gets_one_key_rooted_at_its_directory() {
    let fixtures = tenants(2).await;
    let cache = cache(KeyPolicy::default()).await;

    let key = cache.key(&fixtures[0].tenant).await.unwrap();
    assert_eq!(cache.key(&fixtures[0].tenant).await.unwrap(), key, "a second call gets the cached key");
    assert_eq!(cache.stats(), KeyStats { live: 1, minted: 1, revoked: 0 });

    assert!(writes(&fixtures[0].tenant, &key).await);
    let seen = fixtures[0].tenant.connect(&key).await.unwrap().list("").await.unwrap();
    assert_eq!(seen.entries.len(), 1, "the key sees the tenant's directory, with the one entry it wrote, as its whole tree");
    assert!(fixtures[1].client().await.unwrap().list("").await.unwrap().entries.is_empty(), "the other tenant's directory is untouched");

    let second = cache.key(&fixtures[1].tenant).await.unwrap();
    assert_ne!(second, key);
    assert_eq!(cache.stats().live, 2);
}

#[tokio::test]
async fn callers_that_arrive_together_share_one_mint() {
    let fixtures = tenants(1).await;
    let cache = cache(KeyPolicy::default()).await;
    let keys = join_all((0..32).map(|_| cache.key(&fixtures[0].tenant))).await;
    let first = keys[0].as_ref().unwrap();
    assert!(keys.iter().all(|key| key.as_ref().unwrap() == first));
    assert_eq!(cache.stats().minted, 1);
}

#[tokio::test]
async fn a_read_policy_mints_keys_that_only_read() {
    let fixtures = tenants(1).await;
    let cache = cache(KeyPolicy { grant: Grant::Read, ..KeyPolicy::default() }).await;
    let key = cache.key(&fixtures[0].tenant).await.unwrap();
    assert!(fixtures[0].tenant.connect(&key).await.unwrap().list("").await.is_ok());
    assert!(!writes(&fixtures[0].tenant, &key).await);
}

#[tokio::test]
async fn a_key_near_its_expiry_is_replaced_and_lasts_its_term() {
    let fixtures = tenants(1).await;
    let tenant = &fixtures[0].tenant;
    let cache = cache(KeyPolicy { ttl: Duration::from_secs(6), min_remaining: Duration::from_secs(4), ..KeyPolicy::default() }).await;

    let first = cache.key(tenant).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert_eq!(cache.key(tenant).await.unwrap(), first, "five seconds are left, and four are enough");

    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let second = cache.key(tenant).await.unwrap();
    assert_ne!(second, first, "under four seconds are left");
    assert!(writes(tenant, &first).await, "an operation that holds the first key finishes under it");
    assert!(writes(tenant, &second).await);
    assert_eq!(cache.stats(), KeyStats { live: 2, minted: 2, revoked: 0 });

    // The first key expires six seconds after its mint, and the store deletes it within a second more.
    tokio::time::sleep(Duration::from_millis(5_000)).await;
    assert!(!writes(tenant, &first).await, "the first key has expired");
    assert!(writes(tenant, &second).await);
    assert_eq!(cache.stats().live, 1, "an expired key leaves the count");
}

#[tokio::test]
async fn revoking_a_tenant_ends_its_keys_at_once() {
    let fixtures = tenants(2).await;
    let cache = cache(KeyPolicy::default()).await;
    let held = cache.key(&fixtures[0].tenant).await.unwrap();
    let other = cache.key(&fixtures[1].tenant).await.unwrap();

    cache.revoke(&fixtures[0].tenant).await.unwrap();
    assert!(!writes(&fixtures[0].tenant, &held).await, "a held key is revoked too");
    assert!(writes(&fixtures[1].tenant, &other).await, "another tenant's key stays");
    assert_eq!(cache.stats(), KeyStats { live: 1, minted: 2, revoked: 1 });
    cache.revoke(&fixtures[0].tenant).await.unwrap();
    assert_eq!(cache.stats().revoked, 1, "a second revocation finds nothing to do");

    let fresh = cache.key(&fixtures[0].tenant).await.unwrap();
    assert_ne!(fresh, held);
    assert!(writes(&fixtures[0].tenant, &fresh).await);
}

#[tokio::test]
async fn the_cache_makes_room_by_revoking_the_key_used_longest_ago() {
    let fixtures = tenants(3).await;
    let [a, b, c] = [&fixtures[0].tenant, &fixtures[1].tenant, &fixtures[2].tenant];
    let cache = cache(KeyPolicy { capacity: 2, ..KeyPolicy::default() }).await;

    let key_a = cache.key(a).await.unwrap().to_string();
    let key_b = cache.key(b).await.unwrap().to_string();
    // Tenant `a` is served again, so `b` is the one served longest ago.
    assert_eq!(&*cache.key(a).await.unwrap(), key_a);

    let key_c = cache.key(c).await.unwrap();
    assert!(!writes(b, &key_b).await, "the cache revoked the key of the tenant served longest ago");
    assert!(writes(a, &key_a).await && writes(c, &key_c).await);
    assert_eq!(cache.stats(), KeyStats { live: 2, minted: 3, revoked: 1 });

    // A key an operation holds stays, whenever its tenant was last served.
    let held_a = cache.key(a).await.unwrap();
    let error = cache.key(b).await.unwrap_err();
    assert!(error.to_string().contains("operations hold every one of the cache's 2 keys"), "{error}");
    assert!(writes(a, &held_a).await && writes(c, &key_c).await);
    assert_eq!(cache.stats().live, 2, "a refused call holds no place");

    drop(key_c);
    let key_b = cache.key(b).await.unwrap();
    assert!(writes(b, &key_b).await && writes(a, &held_a).await);
    assert_eq!(cache.stats(), KeyStats { live: 2, minted: 4, revoked: 2 });
}

#[tokio::test]
async fn revoking_everything_ends_every_key() {
    let fixtures = tenants(5).await;
    let cache = cache(KeyPolicy::default()).await;
    let mut keys: Vec<TenantKey> = Vec::new();
    for fixture in &fixtures {
        keys.push(cache.key(&fixture.tenant).await.unwrap());
    }
    cache.revoke_all().await.unwrap();
    for (fixture, key) in fixtures.iter().zip(&keys) {
        assert!(!writes(&fixture.tenant, key).await);
    }
    assert_eq!(cache.stats(), KeyStats { live: 0, minted: 5, revoked: 5 });
}

#[tokio::test]
async fn a_tenant_without_a_directory_holds_no_place() {
    let stack = stack().await;
    let cache = cache(KeyPolicy::default()).await;
    let absent = Tenant { store: stack.inodedb_endpoint.clone(), root: format!("tenants/absent-{:016x}/disk", rand::random::<u64>()), s3: stack.s3("absent") };
    assert!(cache.key(&absent).await.is_err());
    assert_eq!(cache.stats(), KeyStats::default());
}

#[tokio::test]
async fn a_key_the_store_revoked_is_forgotten_on_revoke() {
    let mut fixtures = tenants(1).await;
    let tenant = fixtures[0].tenant.clone();
    let cache = cache(KeyPolicy::default()).await;
    let key = cache.key(&tenant).await.unwrap();

    // Removing a tenant's directory revokes the keys rooted there.
    fixtures.pop().unwrap().remove().await.unwrap();
    assert!(!writes(&tenant, &key).await);
    assert_eq!(cache.key(&tenant).await.unwrap(), key, "the cache has yet to hear of it");

    cache.revoke(&tenant).await.unwrap();
    assert_eq!(cache.stats().live, 0);
    assert!(cache.key(&tenant).await.is_err(), "the directory is gone, so no key can be minted");
    assert_eq!(cache.stats().live, 0);
}

#[tokio::test]
async fn a_cache_serves_the_tenants_of_its_own_store() {
    let fixtures = tenants(1).await;
    let cache = cache(KeyPolicy::default()).await;
    let elsewhere = Tenant { store: "127.0.0.1:1".into(), ..fixtures[0].tenant.clone() };
    let error = cache.key(&elsewhere).await.unwrap_err();
    assert!(error.to_string().contains("127.0.0.1:1"), "{error}");
    assert_eq!(cache.stats().minted, 0);
}

#[tokio::test]
async fn a_policy_leaves_keys_time_to_serve() {
    let admin = stack().await.admin().await.unwrap();
    let short = KeyPolicy { ttl: Duration::from_secs(60), min_remaining: Duration::from_secs(60), ..KeyPolicy::default() };
    assert!(KeyCache::new(admin.clone(), short).unwrap_err().to_string().contains("TTL must exceed"));
    let empty = KeyPolicy { capacity: 0, ..KeyPolicy::default() };
    assert!(KeyCache::new(admin, empty).unwrap_err().to_string().contains("at least one key"));
}
