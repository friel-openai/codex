//! Privacy-safe size telemetry for raw MCP tool catalogs.

use rmcp::model::Tool;
use serde::Serialize;
use std::io::Write;

const RAW_DEFINITION_JSON_BYTES_METRIC: &str =
    "codex.mcp.binding_catalog.raw_definition_json_bytes";
const KIB: f64 = 1024.0;
const MIB: f64 = 1024.0 * KIB;
const RAW_DEFINITION_JSON_BYTES_BUCKETS: &[f64] = &[
    64.0 * KIB,
    128.0 * KIB,
    256.0 * KIB,
    512.0 * KIB,
    768.0 * KIB,
    MIB,
    1.5 * MIB,
    2.0 * MIB,
    3.0 * MIB,
    4.0 * MIB,
    6.0 * MIB,
    8.0 * MIB,
    12.0 * MIB,
    16.0 * MIB,
    24.0 * MIB,
    32.0 * MIB,
    48.0 * MIB,
    64.0 * MIB,
    96.0 * MIB,
    128.0 * MIB,
    192.0 * MIB,
    256.0 * MIB,
    384.0 * MIB,
    512.0 * MIB,
];

pub(crate) fn tool_definition_json_bytes<'a>(
    tools: impl Iterator<Item = &'a Tool>,
    measurement_enabled: bool,
) -> usize {
    if measurement_enabled {
        tools.map(serialized_json_bytes).sum()
    } else {
        0
    }
}

pub(super) fn record_binding_catalog_size(
    metrics: &codex_otel::MetricsClient,
    product_sku: &'static str,
    raw_definition_json_bytes: usize,
) {
    let _ = metrics.histogram_with_boundaries(
        RAW_DEFINITION_JSON_BYTES_METRIC,
        i64::try_from(raw_definition_json_bytes).unwrap_or(i64::MAX),
        RAW_DEFINITION_JSON_BYTES_BUCKETS,
        &[("product_sku", product_sku)],
    );
}

pub(super) fn emit_binding_catalog(
    product_sku: &'static str,
    server_kind: &'static str,
    plugin_id: Option<&str>,
    catalog_source: &'static str,
    tool_count: usize,
    tool_definition_json_bytes: usize,
) {
    tracing::event!(
        target: "codex_otel.trace_safe",
        tracing::Level::INFO,
        event.name = "mcp.binding_catalog",
        product_sku,
        server_kind,
        plugin_id = plugin_id.unwrap_or(""),
        catalog_source,
        tool_count,
        tool_definition_json_bytes,
        "MCP binding catalog materialized"
    );
}

#[derive(Default)]
struct CountingWriter(usize);

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0 += buffer.len();
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialized_json_bytes<T: Serialize + ?Sized>(value: &T) -> usize {
    let mut writer = CountingWriter::default();
    if serde_json::to_writer(&mut writer, value).is_ok() {
        writer.0
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::tool_definition_json_bytes;
    use rmcp::model::JsonObject;
    use rmcp::model::Tool;
    use std::cell::Cell;
    use std::sync::Arc;

    #[test]
    fn tool_definition_json_bytes_skips_disabled_measurement_and_counts_enabled_tools() {
        let tools = ["first", "second"]
            .map(|name| Tool::new(name, "tool description", Arc::new(JsonObject::default())));
        let visited = Cell::new(0);
        let observed_tools = || tools.iter().inspect(|_| visited.set(visited.get() + 1));

        assert_eq!(tool_definition_json_bytes(observed_tools(), false), 0);
        assert_eq!(visited.get(), 0);

        let expected_bytes = tools
            .iter()
            .map(|tool| serde_json::to_vec(tool).expect("serialized tool").len())
            .sum::<usize>();
        assert_eq!(
            tool_definition_json_bytes(observed_tools(), true),
            expected_bytes
        );
        assert_eq!(visited.get(), tools.len());
        assert!(expected_bytes > 0);
    }
}
