use crate::{
    WhoopError, WhoopPacket,
    constants::{CommandNumber, MetadataType, PacketType},
    helpers::BufferReader,
};

mod history;
pub use history::{Activity, HistoryReading, ParsedHistoryReading};

#[derive(Debug, PartialEq)]
pub enum WhoopData {
    HistoryReading(HistoryReading),
    HistoryMetadata {
        unix: u32,
        temperature: f32,
        data: u32,
        cmd: MetadataType,
    },
    ConsoleLog {
        unix: u32,
        log: String,
    },
    RunAlarm {
        unix: u32,
    },
    Event {
        unix: u32,
        event: CommandNumber,
    },
    UnknownEvent {
        unix: u32,
        event: u8,
    },
    VersionInfo {
        harvard: String,
        boylston: String,
    },
}

impl WhoopData {
    pub fn from_packet(packet: WhoopPacket) -> Result<Self, WhoopError> {
        match packet.packet_type {
            PacketType::HistoricalData => Self::parse_historical_packet(packet.data),
            PacketType::Metadata => Self::parse_metadata(packet),
            PacketType::ConsoleLogs => Self::parse_console_log(packet.data),
            PacketType::Event => Self::parse_event(packet),
            PacketType::CommandResponse => {
                let command = CommandNumber::from_u8(packet.cmd)
                    .ok_or(WhoopError::InvalidCommandType(packet.cmd))?;

                match command {
                    CommandNumber::ReportVersionInfo => {
                        Self::parse_report_version_info(packet.data)
                    }
                    _ => Err(WhoopError::Unimplemented),
                }
            }
            _ => Err(WhoopError::Unimplemented),
        }
    }

    fn parse_event(mut packet: WhoopPacket) -> Result<Self, WhoopError> {
        let command = CommandNumber::from_u8(packet.cmd).ok_or(packet.cmd);

        let _ = packet.data.pop_front()?;
        let unix = packet.data.read_u32_le()?;

        match command {
            Ok(CommandNumber::RunAlarm) => Ok(Self::RunAlarm { unix }),
            Ok(CommandNumber::SendR10R11Realtime)
            | Ok(CommandNumber::ToggleRealtimeHr)
            | Ok(CommandNumber::GetClock)
            | Ok(CommandNumber::RebootStrap)
            | Ok(CommandNumber::ToggleR7DataCollection)
            | Ok(CommandNumber::ToggleGenericHrProfile) => Ok(Self::Event {
                unix,
                event: command.expect("We check above that it is `Ok`"),
            }),
            Err(unknown) => Ok(Self::UnknownEvent {
                unix,
                event: unknown,
            }),
            _ => Err(WhoopError::Unimplemented),
        }
    }

    fn parse_console_log(mut packet: Vec<u8>) -> Result<Self, WhoopError> {
        let _ = packet.pop_front()?;
        let unix = packet.read_u32_le()?;

        let _ = packet.read::<2>();

        let mut result = Vec::new();

        let mut iter = packet.iter();
        let lookahead = packet.windows(3);

        for window in lookahead {
            if window != [0x34, 0x00, 0x01] {
                result.push(iter.next().copied().unwrap_or_default());
            } else {
                iter.nth(2);
            }
        }

        result.extend(iter);
        // not sure why this happens but sometimes Whoop gives logs
        // where part of logs is invalid, but some info can be still gained from them
        let lossy = String::from_utf8_lossy(&result).to_string();
        let log = match String::from_utf8(result) {
            Ok(log) => log,
            Err(_) => lossy,
        };
        Ok(Self::ConsoleLog { unix, log })
    }

    fn parse_metadata(mut packet: WhoopPacket) -> Result<Self, WhoopError> {
        let cmd =
            MetadataType::from_u8(packet.cmd).ok_or(WhoopError::InvalidMetadataType(packet.cmd))?;

        let unix = packet.data.read_u32_le()?;
        let temp_bytes = packet.data.read::<6>()?;
        let mut temp_u64_bytes = [0u8; 8];
        temp_u64_bytes[..6].copy_from_slice(&temp_bytes);
        let temp_raw = u64::from_le_bytes(temp_u64_bytes);
        let temperature = temp_raw as f32 / 100000.0;
        let data = packet.data.read_u32_le()?;

        Ok(Self::HistoryMetadata { unix, temperature, data, cmd })
    }

    fn parse_historical_packet(mut packet: Vec<u8>) -> Result<Self, WhoopError> {
        let _something = packet.read::<4>();
        let unix = packet.read_u32_le()?;
        let _something = packet.read::<6>();
        let bpm = packet.pop_front()?;
        let rr_count = packet.pop_front()?;
        let mut rr = Vec::new();
        for _ in 0..4 {
            let rr_ = packet.read_u16_le()?;
            if rr_ == 0 {
                continue;
            }
            rr.push(rr_);
        }
        if rr.len() as u8 != rr_count {
            return Err(WhoopError::InvalidData);
        }

        let activity = packet.read_u32_le()?;

        Ok(Self::HistoryReading(HistoryReading {
            unix,
            bpm,
            rr,
            activity,
        }))
    }

    fn parse_report_version_info(mut data: Vec<u8>) -> Result<Self, WhoopError> {
        let _ = data.read::<3>();
        let h_major = data.read_u32_le()?;
        let h_minor = data.read_u32_le()?;
        let h_patch = data.read_u32_le()?;
        let h_build = data.read_u32_le()?;
        let b_major = data.read_u32_le()?;
        let b_minor = data.read_u32_le()?;
        let b_patch = data.read_u32_le()?;
        let b_build = data.read_u32_le()?;
        Ok(Self::VersionInfo {
            harvard: format!("{}.{}.{}.{}", h_major, h_minor, h_patch, h_build),
            boylston: format!("{}.{}.{}.{}", b_major, b_minor, b_patch, b_build),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        WhoopPacket,
        constants::{MetadataType, PacketType},
        whoop_data::{WhoopData, history::HistoryReading},
    };

    #[test]
    fn parse_historical_packet() {
        let data = hex::decode("aa5c00f02f0c050f0008029e7e2868906380542c01400000000000000000000021436dff904d893dec19fb3e5ccf9b3d0a03773f00000000ec19fb3e5ccf9b3d0a03773fe0015702eb02590239019004010c020c310000000000000115f49cd0").expect("Invalid hex data");
        let packet = WhoopPacket::from_data(data).expect("Invalid packet data");
        let data = WhoopData::from_packet(packet).expect("Invalid packet");

        assert_eq!(
            data,
            WhoopData::HistoryReading(HistoryReading {
                unix: 1747484318,
                bpm: 64,
                rr: vec![],
                activity: 1833115904
            })
        );

        let data = hex::decode("aa5c00f02f0c053f940900da106966280080545401360195040000000000000000a34cff0050bf3b144efb3da4a4463f299c0dbf00004c42144efb3da4a4463f299c0dbff40155023b03530255016004010c020c2000000000000002e8c17c8d").expect("Invalid hex data");
        let packet = WhoopPacket::from_data(data).expect("Invalid packet data");
        let data = WhoopData::from_packet(packet).expect("Invalid packet");

        assert_eq!(
            data,
            WhoopData::HistoryReading(HistoryReading {
                unix: 1718161626,
                bpm: 54,
                rr: vec![1173],
                activity: 1285750784
            })
        );

        let data = hex::decode("aa6400a12f1805cb6cc100f7715c67300b805454015700000000000000000000005161cda013a03dcdcc1cbbd723133ee146873f00028a46cdcc1cbbd723133ee146873f28026d029c03700257019004010c020c3000000000000001b9120000000000000a9c4cac").expect("Invalid hex data");
        let packet = WhoopPacket::from_data(data).expect("Invalid packet data");
        let data = WhoopData::from_packet(packet).expect("Invalid packet");

        assert_eq!(
            data,
            WhoopData::HistoryReading(HistoryReading {
                unix: 1734111735,
                bpm: 87,
                rr: Vec::new(),
                activity: 1632698368
            })
        );

        let data = hex::decode("aa1c00ab31370268ae7667702d32000000c7b6000010000000000000e01eba47")
            .expect("Invalid hex data");

        let packet = WhoopPacket::from_data(data).expect("Invalid packet data");
        let data = WhoopData::from_packet(packet).expect("Invalid packet");

        assert_eq!(
            data,
            WhoopData::HistoryMetadata {
                unix: 1735831144,
                temperature: 32.88432_f32,
                data: 46791,
                cmd: MetadataType::HistoryEnd
            }
        );
    }

    #[test]
    fn parse_console_logs() {
        let packet = WhoopPacket{
            packet_type: PacketType::ConsoleLogs,
            seq: 0,
            cmd: 2,
            data: hex::decode("007e0b6d67907b340001205472696d3a20307830303030303030303a30303031623665662028303a313132333637290a3231312c203131323633313400").expect("Invalid hex data"),
        };

        let data = WhoopData::from_packet(packet).expect("Invalid data");
        assert_eq!(
            data,
            WhoopData::ConsoleLog {
                unix: 1735199614,
                log: " Trim: 0x00000000:0001b6ef (0:112367)\n211, 1126314\0".to_owned()
            }
        )
    }

    #[test]
    fn parse_event() {
        let packet = WhoopPacket {
            packet_type: PacketType::Event,
            seq: 0,
            cmd: 68,
            data: hex::decode("00b70c5467000c04000101ff00").expect("Invalid hex data"),
        };

        let data = WhoopData::from_packet(packet).expect("Invalid data");

        assert_eq!(data, WhoopData::RunAlarm { unix: 1733561527 });

        dbg!(data);
    }

    #[test]
    fn parse_metadata() {
        let bytes = hex::decode("aa1c00ab311002a9fc8367205337000000257e00000a0000000000007ac020f8")
            .expect("invalid bytes");
        let packet = WhoopPacket::from_data(bytes).expect("Invalid packet");
        let data = WhoopData::from_packet(packet).expect("invalid packet");
        assert_eq!(
            data,
            WhoopData::HistoryMetadata {
                unix: 1736703145,
                temperature: 36.2576_f32,
                data: 32293,
                cmd: MetadataType::HistoryEnd
            }
        );

        let bytes = hex::decode("aa2c005231010146fb8367404c0600000010000000020000002900000010000000030000000000000008020055fd251d").expect("invalid bytes");
        let packet = WhoopPacket::from_data(bytes).expect("Invalid packet");
        let data = WhoopData::from_packet(packet).expect("invalid packet");
        assert_eq!(
            data,
            WhoopData::HistoryMetadata {
                unix: 1736702790,
                temperature: 4.12736_f32,
                data: 16,
                cmd: MetadataType::HistoryStart,
            }
        );
    }

    #[test]
    fn parse_version_response() {
        let response = hex::decode("aa50000c2477070a01012900000011000000020000000000000011000000020000000200000000000000030000000400000000000000000000000300000006000000000000000000000008050100000074b95569").expect("invalid data");
        let packet = WhoopPacket::from_data(response).expect("invalid packet");
        let data = WhoopData::from_packet(packet).expect("invalid packet");
        assert_eq!(
            data,
            WhoopData::VersionInfo {
                harvard: String::from("41.17.2.0"),
                boylston: String::from("17.2.2.0")
            }
        )
    }
}
