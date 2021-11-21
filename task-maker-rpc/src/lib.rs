use futures::future::BoxFuture;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::io::Cursor;
use std::sync::Arc;

pub use task_maker_rpc_macro::service;

pub trait MessageHandler {
    fn handle(
        &self,
        connector: &Arc<Connector>,
        input: [u8],
        write_to: &mut Cursor<Vec<u8>>,
    ) -> BoxFuture<'_, ()>;
}

pub struct Connector {
    // TODO
}

impl Connector {
    fn uuid_of<T>(&self, address: &Address<T>) -> (u64, u64) {
        match address {
            Address::Local(_) => (0, 0),
            Address::Remote {
                remote_id: sid,
                object_id: oid,
                ..
            } => (*sid, *oid),
        }
    }
}

pub enum Address<T> {
    Local(Arc<T>),
    Remote {
        connector: Arc<Connector>,
        remote_id: u64,
        object_id: u64,
    },
}

pub trait DeserializeFromConnector<'de> {
    fn deserialize_from_connector<D>(
        deserializer: D,
        connector: &Arc<Connector>,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
        Self: Sized;
}

impl<'de, T> DeserializeFromConnector<'de> for T
where
    T: Deserialize<'de> + Sized,
{
    fn deserialize_from_connector<D>(
        deserializer: D,
        _connector: &Arc<Connector>,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer)
    }
}

impl<'de, T> DeserializeFromConnector<'de> for Address<T> {
    fn deserialize_from_connector<D>(
        deserializer: D,
        connector: &Arc<Connector>,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let id = <(u64, u64)>::deserialize(deserializer)?;
        Ok(Address::<T>::Remote {
            connector: connector.clone(),
            remote_id: id.0,
            object_id: id.1,
        })
    }
}

pub trait SerializeFromConnector {
    fn serialize_from_connector<S>(
        &self,
        serializer: S,
        connector: &Arc<Connector>,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        Self: Sized;
}

impl<T> SerializeFromConnector for T
where
    T: Serialize + Sized,
{
    fn serialize_from_connector<S>(
        &self,
        serializer: S,
        _connector: &Arc<Connector>,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialize(serializer)
    }
}

impl<T> SerializeFromConnector for Address<T> {
    fn serialize_from_connector<S>(
        &self,
        serializer: S,
        connector: &Arc<Connector>,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        connector.uuid_of(self).serialize(serializer)
    }
}
