"""Industrial IoT sensor telemetry & predictive maintenance pipeline.

Processes high-frequency turbine vibration, thermal, and pressure signals.
Computes Root Mean Square (RMS) vibration energy, temperature gradients,
and machine health degradation indexes.
"""

import os
import random
import tempfile
from typing import List, Tuple

import polars as pl

from biflux.core import BifluxContext, BifluxPipeline, Environment, ExecutionMode


class PredictiveMaintenancePipeline(BifluxPipeline):
    """Predictive maintenance feature engineering for industrial machinery."""

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(
                vibration_energy=(
                    pl.col("vibration_x").pow(2) + pl.col("vibration_y").pow(2)
                ).sqrt(),
                thermal_stress=(pl.col("temperature_c") - 75.0).clip(0.0, 100.0),
                pressure_variance=(pl.col("pressure_bar") - 30.0).abs(),
            )
            .with_columns(
                health_degradation_index=(
                    (pl.col("vibration_energy") / 10.0) * 0.45
                    + (pl.col("thermal_stress") / 50.0) * 0.35
                    + (pl.col("pressure_variance") / 10.0) * 0.20
                )
                .clip(0.0, 1.0)
                .round(6),
            )
            .group_by("turbine_id")
            .agg(
                telemetry_readings=pl.len(),
                avg_vibration_energy=pl.col("vibration_energy").mean().round(4),
                max_vibration_energy=pl.col("vibration_energy").max().round(4),
                avg_temperature_c=pl.col("temperature_c").mean().round(2),
                max_temperature_c=pl.col("temperature_c").max().round(2),
                max_degradation_index=pl.col("health_degradation_index").max().round(6),
                maintenance_alerts_triggered=(pl.col("health_degradation_index") >= 0.65)
                .sum()
                .cast(pl.Int64),
            )
            .sort("turbine_id")
        )


def generate_synthetic_telemetry(n_readings: int = 20_000) -> pl.DataFrame:
    """Generate industrial sensor readings."""
    random.seed(4242)
    turbines = [f"TURBINE-UNIT-{i:03d}" for i in range(1, 11)]

    data: List[Tuple[str, float, float, float, float, int, int]] = []
    base_ts = 1_720_000_000_000

    for i in range(n_readings):
        tid = turbines[i % len(turbines)]
        vib_x = round(max(0.1, random.gauss(2.5, 0.8)), 3)
        vib_y = round(max(0.1, random.gauss(2.7, 0.9)), 3)
        temp_c = round(random.gauss(82.0, 6.5), 2)
        pressure = round(random.gauss(30.2, 1.8), 2)
        rpm = random.randint(3400, 3600)
        ts = base_ts + i * 100
        data.append((tid, vib_x, vib_y, temp_c, pressure, rpm, ts))

    return pl.DataFrame(
        data,
        schema={
            "turbine_id": pl.Utf8,
            "vibration_x": pl.Float64,
            "vibration_y": pl.Float64,
            "temperature_c": pl.Float64,
            "pressure_bar": pl.Float64,
            "rpm": pl.Int64,
            "timestamp": pl.Int64,
        },
        orient="row",
    )


def run_iot_telemetry_demo() -> bool:
    print("=" * 80)
    print("  PROJECT BIFLUX: INDUSTRIAL IOT TELEMETRY & MAINTENANCE PIPELINE")
    print("=" * 80)

    n_readings = 20_000
    print(f"[*] Generating {n_readings:,} industrial telemetry records for 10 turbines...")
    telemetry_df = generate_synthetic_telemetry(n_readings=n_readings)
    print(f"[*] Telemetry Sample:\n{telemetry_df.head(3)}\n")

    with tempfile.TemporaryDirectory() as workdir:
        raw_telemetry_path = os.path.join(workdir, "turbine_telemetry.parquet")
        sink_path = os.path.join(workdir, "turbine_health_features.parquet")
        telemetry_df.write_parquet(raw_telemetry_path)

        # Batch
        batch_ctx = BifluxContext(mode=ExecutionMode.BATCH, env=Environment.LOCAL)
        batch_pipeline = PredictiveMaintenancePipeline(batch_ctx)
        b_res = batch_pipeline.run(
            source_uri=raw_telemetry_path, sink_uri=sink_path, sample_df=telemetry_df.lazy()
        )
        batch_output = pl.read_parquet(sink_path).sort("turbine_id")
        print(f"[✓] Batch Telemetry Processed in {b_res.duration_ms} ms (status={b_res.status})")
        print(f"[✓] Batch Health Profiles:\n{batch_output}\n")

        # Stream
        live_ctx = BifluxContext(mode=ExecutionMode.LIVE, kafka_brokers="localhost:9092")
        live_pipeline = PredictiveMaintenancePipeline(live_ctx)
        l_res = live_pipeline.run(
            source_uri="iot.sensor.stream",
            sink_uri="features.turbine.health",
            sample_df=telemetry_df.lazy(),
        )
        live_output = live_pipeline.process_micro_batch(telemetry_df).sort("turbine_id")
        print(
            f"[✓] Streaming Telemetry Processed in {l_res.duration_ms} ms (status={l_res.status})"
        )

        # Verify
        print("=" * 80)
        print(">>> [THE PROOF] IoT Sensor Feature Parity Verification")
        print("=" * 80)
        table_hdr = (
            f"{'TURBINE ID':<18} | {'BATCH DEGRAD':<14} | "
            f"{'LIVE DEGRAD':<14} | {'DIFF':<10} | STATUS"
        )
        print(table_hdr)
        print("-" * 80)

        all_matched = True
        for b_row, l_row in zip(
            batch_output.iter_rows(named=True), live_output.iter_rows(named=True)
        ):
            tid = b_row["turbine_id"]
            b_deg = b_row["max_degradation_index"]
            l_deg = l_row["max_degradation_index"]
            diff = abs(b_deg - l_deg)
            match = diff < 1e-6
            if not match:
                all_matched = False
            status = "MATCH (0 Skew)" if match else "MISMATCH"
            print(f"{tid:<18} | {b_deg:<14.6f} | {l_deg:<14.6f} | {diff:<10.6f} | {status}")

        print("-" * 80)
        if all_matched:
            print(">>> VERIFICATION SUCCESS: Zero Train-Serve Skew in IoT Telemetry Pipeline.")
        return all_matched


if __name__ == "__main__":
    run_iot_telemetry_demo()
