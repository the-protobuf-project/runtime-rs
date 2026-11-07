//! MQTT. `plain` is docker/compose.mqtt.yaml.

use streams::{
    Registry,
    drivers::mqtt::{MqttConfig, MqttOptions, MqttProvider, QoS},
};

use super::{Conn, Error, env, unknown_profile};

pub async fn connect(profile: &str, registry: Registry) -> Result<Conn, Error> {
    let options = match profile {
        "plain" => {
            let port = env("MQTT_PORT", "11883").parse()?;
            MqttOptions::new("bench", env("MQTT_HOST", "127.0.0.1"), port)
        }
        other => return Err(unknown_profile("mqtt", other)),
    };
    let config = MqttConfig { prefix: "bench".into(), qos: QoS::AtLeastOnce, registry, ..MqttConfig::default() };
    Ok(Conn::one(MqttProvider::connect(options, config).await?))
}
