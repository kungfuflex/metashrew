use anyhow::{Result};
use metashrew_runtime::{BatchLike, KeyValueStoreLike};
use redis::Commands;
use std::sync::{Arc, Mutex};

const TIP_HEIGHT_KEY: &'static str = "/__INTERNAL/tip-height";
static mut _HEIGHT: u32 = 0;

pub struct RedisRuntimeAdapter(pub String, pub Arc<Mutex<redis::Connection>>);

pub async fn query_height(connection: &mut redis::Connection, start_block: u32) -> Result<u32> {
    let bytes: Vec<u8> = match connection.get(&TIP_HEIGHT_KEY.as_bytes().to_vec()) {
        Ok(v) => v,
        Err(_) => {
            return Ok(start_block);
        }
    };
    if bytes.len() == 0 {
        return Ok(start_block);
    }
    let bytes_ref: &[u8] = &bytes;
    Ok(u32::from_le_bytes(bytes_ref.try_into().unwrap()))
}

impl RedisRuntimeAdapter {
    pub fn connect(&self) -> Result<redis::Connection> {
        Ok(redis::Client::open(self.0.clone())?.get_connection()?)
    }
    pub fn open(redis_uri: String) -> Result<RedisRuntimeAdapter> {
        Ok(RedisRuntimeAdapter(
            redis_uri.clone(),
            Arc::new(Mutex::new(
                redis::Client::open(redis_uri.clone())?.get_connection()?,
            )),
        ))
    }
    pub fn reset_connection(&mut self) {
        self.1 = Arc::new(Mutex::new(self.connect().unwrap()));
    }
}

pub struct RedisBatch(pub redis::Pipeline);

fn to_redis_args<T: AsRef<[u8]>>(v: T) -> Vec<Vec<u8>> {
    return vec![v.as_ref().try_into().unwrap()];
}

impl BatchLike for RedisBatch {
    fn default() -> Self {
        Self(redis::pipe())
    }
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, k: K, v: V) {
        self.0
            .cmd("SET")
            .arg(to_redis_args(k))
            .arg(to_redis_args(v))
            .ignore();
    }
}

impl Clone for RedisRuntimeAdapter {
    fn clone(&self) -> Self {
        return Self(self.0.clone(), self.1.clone());
    }
}

impl KeyValueStoreLike for RedisRuntimeAdapter {
    type Batch = RedisBatch;
    type Error = redis::RedisError;
    fn write(&mut self, batch: RedisBatch) -> Result<(), Self::Error> {
        let key_bytes: Vec<u8> = TIP_HEIGHT_KEY.as_bytes().to_vec();
        let height_bytes: Vec<u8> = (unsafe { _HEIGHT }).to_le_bytes().to_vec();
        let mut connection = self.connect().unwrap();
        let _ok: bool = connection
            .set(to_redis_args(&key_bytes), to_redis_args(&height_bytes))
            .unwrap();
        let result = batch.0.query(&mut connection);
        self.reset_connection();
        result
    }
    fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Self::Error> {
        self.1.lock().unwrap().get(to_redis_args(key))
    }
    fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Self::Error> {
        self.connect().unwrap().del(to_redis_args(key))
    }
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<(), Self::Error> {
        self.1
            .lock()
            .unwrap()
            .set(to_redis_args(key), to_redis_args(value))
    }
}