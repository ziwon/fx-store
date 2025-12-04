use crate::types::OHLCV;
use parking_lot::RwLock;
use std::sync::Arc;
use zstd::bulk::{compress, decompress};

const BLOCK_SIZE: usize = 1440; // 1일 = 1440분

/// 압축된 일일 블록
#[derive(Clone)]
pub struct CompressedBlock {
    pub date: u32, // YYYYMMDD
    pub symbol_id: u16,
    pub data: Arc<Vec<u8>>,
    cached: Arc<RwLock<Option<Box<[OHLCV; BLOCK_SIZE]>>>>,
}

impl CompressedBlock {
    pub fn new(date: u32, symbol_id: u16, records: &[OHLCV]) -> Self {
        let mut block = Box::new([OHLCV::default(); BLOCK_SIZE]);

        // 1분 간격으로 정렬
        for rec in records {
            let minute_of_day = ((rec.ts / 1_000_000_000) % 86400) / 60;
            block[minute_of_day as usize] = *rec;
        }

        // 압축 (레벨 3이 속도/압축률 균형 최적)
        let serialized = bincode::serialize(&block.to_vec()).unwrap();
        let compressed = compress(&serialized, 3).unwrap();

        Self {
            date,
            symbol_id,
            data: Arc::new(compressed),
            cached: Arc::new(RwLock::new(None)),
        }
    }

    pub fn decompress(&self) -> Box<[OHLCV; BLOCK_SIZE]> {
        // 캐시 확인
        if let Some(cached) = self.cached.read().as_ref() {
            return cached.clone();
        }

        // 압축 해제
        // bincode serialization of Vec adds 8 bytes for length
        let decompressed = decompress(&self.data, BLOCK_SIZE * 40 + 8).unwrap();
        let records: Vec<OHLCV> = bincode::deserialize(&decompressed).unwrap();
        let mut block = Box::new([OHLCV::default(); BLOCK_SIZE]);
        for (i, record) in records.into_iter().enumerate() {
            if i < BLOCK_SIZE {
                block[i] = record;
            }
        }

        // 캐시 저장
        *self.cached.write() = Some(block.clone());
        block
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_compression() {
        let mut records = Vec::new();
        // Create a dummy record at 12:00:00 (720th minute)
        // 2023-01-01 12:00:00 = 1672574400
        let ts = 1672574400 * 1_000_000_000;
        let rec = OHLCV {
            ts,
            open: 100000,
            high: 100000,
            low: 100000,
            close: 100000,
            volume: 100,
            symbol_id: 1,
            _pad: [0; 10],
        };
        records.push(rec);

        let block = CompressedBlock::new(20230101, 1, &records);
        
        // Decompress and verify
        let decompressed = block.decompress();
        
        // 12:00 is the 720th minute of the day
        let target_idx = 720;
        let ts_val = decompressed[target_idx].ts;
        let vol_val = decompressed[target_idx].volume;
        
        assert_eq!(ts_val, ts);
        assert_eq!(vol_val, 100);
        
        // Verify empty slot is default
        let empty_ts = decompressed[0].ts;
        assert_eq!(empty_ts, 0);
    }
}
