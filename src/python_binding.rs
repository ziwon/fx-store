use pyo3::prelude::*;
use crate::store::FxStore;
use crate::types::OHLCV;
use std::sync::Arc;

#[pyclass]
struct PyFxStore {
    inner: Arc<FxStore>,
}

#[pymethods]
impl PyFxStore {
    #[new]
    fn new() -> Self {
        PyFxStore {
            inner: Arc::new(FxStore::new()),
        }
    }

    fn import_csv(&self, path: &str, symbol: &str) -> PyResult<()> {
        self.inner.import_csv(path, symbol).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })
    }

    fn query_range(&self, symbol: &str, start_ts: u64, end_ts: u64) -> PyResult<Vec<PyOHLCV>> {
        let records = self.inner.query_range(symbol, start_ts, end_ts);
        Ok(records.map(PyOHLCV::from).collect())
    }
    
    fn get_symbols(&self) -> Vec<String> {
        self.inner.get_symbols()
    }
}

#[pyclass]
#[derive(Clone)]
struct PyOHLCV {
    #[pyo3(get)]
    ts: u64,
    #[pyo3(get)]
    open: f64,
    #[pyo3(get)]
    high: f64,
    #[pyo3(get)]
    low: f64,
    #[pyo3(get)]
    close: f64,
    #[pyo3(get)]
    volume: u32,
}

impl From<OHLCV> for PyOHLCV {
    fn from(r: OHLCV) -> Self {
        PyOHLCV {
            ts: r.ts,
            open: r.open as f64 / 100000.0,
            high: r.high as f64 / 100000.0,
            low: r.low as f64 / 100000.0,
            close: r.close as f64 / 100000.0,
            volume: r.volume,
        }
    }
}

#[pymodule]
fn fx_store(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyFxStore>()?;
    m.add_class::<PyOHLCV>()?;
    Ok(())
}
