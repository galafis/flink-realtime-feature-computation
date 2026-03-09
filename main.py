"""
Flink Real-Time Feature Computation Engine - Demo

Demonstrates the complete feature computation pipeline by:
1. Generating realistic e-commerce events (page views, searches, purchases, etc.)
2. Processing events through a stream processor with watermark tracking
3. Computing user-level features in real-time via windowed aggregations
4. Detecting complex event patterns (cart abandonment, high spenders)
5. Storing features in an online/offline feature store
6. Reporting metrics and computed features

Run with: python main.py

Author: Gabriel Demetrios Lafis
"""

import sys
import time
import os
from datetime import datetime, timezone

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.stream.source import KafkaSourceSimulator, StreamEvent
from src.stream.processor import StreamProcessor, TimeCharacteristic
from src.stream.sink import FeatureStoreSink, SinkRecord
from src.features.feature_pipeline import FeaturePipeline, FeatureDefinition, ComputedFeature
from src.features.cep import (
    ComplexEventProcessor,
    PatternDefinition,
    Alert,
)
from src.store.feature_store import FeatureStore
from src.monitoring.metrics import StreamMetrics


# ── Terminal colors (cross-platform safe) ──────────────────────────────
class Colors:
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    END = "\033[0m"


def c(text: str, color: str) -> str:
    return f"{color}{text}{Colors.END}"


def print_banner():
    banner = r"""
    ╔══════════════════════════════════════════════════════════════════╗
    ║                                                                ║
    ║    ███████╗██╗     ██╗███╗   ██╗██╗  ██╗                       ║
    ║    ██╔════╝██║     ██║████╗  ██║██║ ██╔╝                       ║
    ║    █████╗  ██║     ██║██╔██╗ ██║█████╔╝                        ║
    ║    ██╔══╝  ██║     ██║██║╚██╗██║██╔═██╗                        ║
    ║    ██║     ███████╗██║██║ ╚████║██║  ██╗                       ║
    ║    ╚═╝     ╚══════╝╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝                     ║
    ║                                                                ║
    ║    Real-Time Feature Computation Engine                        ║
    ║    Stream Processing + Feature Store + CEP                     ║
    ║                                                                ║
    ╚══════════════════════════════════════════════════════════════════╝
    """
    print(c(banner, Colors.CYAN))


def print_section(title: str):
    print(f"\n{c('━' * 66, Colors.DIM)}")
    print(f"  {c(title, Colors.BOLD + Colors.BLUE)}")
    print(f"{c('━' * 66, Colors.DIM)}\n")


def format_timestamp(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def main():
    print_banner()

    # ── 1. Initialize Components ──────────────────────────────────────
    print_section("1. Initializing Pipeline Components")

    # Source: generates realistic e-commerce events
    source = KafkaSourceSimulator(
        num_users=20,
        events_per_second=50.0,
        late_event_probability=0.08,
        max_late_seconds=15,
        seed=42,
    )
    print(f"  {c('[OK]', Colors.GREEN)} Kafka Source Simulator (20 users, 50 events/s)")

    # Stream Processor: event-time processing with watermarks
    processor = StreamProcessor(
        time_characteristic=TimeCharacteristic.EVENT_TIME,
        max_out_of_orderness_seconds=5.0,
        allowed_lateness_seconds=30.0,
    )
    processor.key_by(lambda e: e.user_id)
    print(f"  {c('[OK]', Colors.GREEN)} Stream Processor (event-time, watermark=5s)")

    # Feature Store: in-memory online + file-based offline
    store = FeatureStore(
        default_ttl_seconds=3600,
        enable_offline=False,  # no file I/O for demo
        enable_versioning=True,
    )
    print(f"  {c('[OK]', Colors.GREEN)} Feature Store (online + versioning)")

    # Feature Pipeline: register feature definitions
    computed_features_log: list = []

    def on_feature_computed(feat: ComputedFeature):
        computed_features_log.append(feat)
        store.set_features(
            entity_id=feat.entity_id,
            features={feat.feature_name: feat.feature_value},
            timestamp=feat.computed_at,
        )

    pipeline = FeaturePipeline(
        trigger_interval_seconds=0.0,  # trigger on every watermark advance
        on_feature_computed=on_feature_computed,
    )

    # Register feature definitions
    features_to_register = [
        FeatureDefinition(
            name="page_view_count_5m",
            aggregation="count",
            window_type="tumbling",
            window_size_seconds=300,
            source_field="_count",
            filter_event_type="page_view",
            description="Page views in last 5 minutes",
        ),
        FeatureDefinition(
            name="search_count_5m",
            aggregation="count",
            window_type="tumbling",
            window_size_seconds=300,
            source_field="_count",
            filter_event_type="search",
            description="Searches in last 5 minutes",
        ),
        FeatureDefinition(
            name="total_spend_5m",
            aggregation="sum",
            window_type="tumbling",
            window_size_seconds=300,
            source_field="total_amount",
            filter_event_type="purchase",
            description="Total spending in last 5 minutes",
        ),
        FeatureDefinition(
            name="avg_spend_per_purchase_5m",
            aggregation="avg",
            window_type="tumbling",
            window_size_seconds=300,
            source_field="total_amount",
            filter_event_type="purchase",
            description="Average purchase amount in 5 min",
        ),
        FeatureDefinition(
            name="cart_additions_5m",
            aggregation="count",
            window_type="tumbling",
            window_size_seconds=300,
            source_field="_count",
            filter_event_type="add_to_cart",
            description="Cart additions in last 5 minutes",
        ),
        FeatureDefinition(
            name="distinct_products_viewed_5m",
            aggregation="distinct_count",
            window_type="tumbling",
            window_size_seconds=300,
            source_field="_count",
            filter_event_type="product_click",
            description="Distinct products viewed in 5 min",
        ),
        FeatureDefinition(
            name="max_cart_value_5m",
            aggregation="max",
            window_type="tumbling",
            window_size_seconds=300,
            source_field="cart_total",
            filter_event_type="checkout_start",
            description="Maximum cart value in 5 minutes",
        ),
        FeatureDefinition(
            name="session_event_count",
            aggregation="count",
            window_type="session",
            window_size_seconds=0,
            session_gap_seconds=120,
            source_field="_count",
            description="Events per user session",
        ),
    ]

    for feat_def in features_to_register:
        pipeline.register_feature(feat_def)

    print(f"  {c('[OK]', Colors.GREEN)} Feature Pipeline ({len(features_to_register)} features registered)")

    # CEP: complex event patterns
    alerts_log: list = []

    def on_alert(alert: Alert):
        alerts_log.append(alert)

    cep = ComplexEventProcessor(on_alert=on_alert)

    cep.register_pattern(PatternDefinition(
        name="cart_abandonment",
        pattern_type="absence",
        event_types=["add_to_cart", "purchase"],
        time_window_seconds=120,
        description="Cart added but no purchase within 2 min",
    ))

    cep.register_pattern(PatternDefinition(
        name="high_value_purchase",
        pattern_type="threshold",
        event_types=["purchase"],
        time_window_seconds=300,
        threshold_field="total_amount",
        threshold_value=200.0,
        description="Purchase exceeding $200",
    ))

    cep.register_pattern(PatternDefinition(
        name="rapid_searches",
        pattern_type="frequency",
        event_types=["search"],
        time_window_seconds=60,
        min_occurrences=5,
        description="5+ searches within 1 minute",
    ))

    cep.register_pattern(PatternDefinition(
        name="browse_to_purchase",
        pattern_type="sequence",
        event_types=["product_click", "add_to_cart", "purchase"],
        time_window_seconds=300,
        description="Full purchase funnel within 5 min",
    ))

    print(f"  {c('[OK]', Colors.GREEN)} Complex Event Processor (4 patterns)")

    # Metrics collector
    metrics = StreamMetrics(report_interval_seconds=5.0)
    print(f"  {c('[OK]', Colors.GREEN)} Metrics Collector")

    # Sink
    sink = FeatureStoreSink(feature_store=store)
    sink.open()
    print(f"  {c('[OK]', Colors.GREEN)} Feature Store Sink")

    # ── 2. Generate and Process Events ────────────────────────────────
    print_section("2. Generating & Processing E-Commerce Events")

    NUM_EVENTS = 500
    events = source.generate_events(NUM_EVENTS)

    # Sort by timestamp for proper event-time processing
    events.sort(key=lambda e: e.timestamp)

    # Shift timestamps so windows trigger properly during the demo:
    # We set the first event to 5 minutes ago so tumbling windows close.
    time_offset = events[0].timestamp
    base_time = time.time() - 600  # 10 minutes ago
    for evt in events:
        evt.timestamp = base_time + (evt.timestamp - time_offset)

    print(f"  Generated {c(str(NUM_EVENTS), Colors.YELLOW)} events across"
          f" {c(str(source.num_users), Colors.YELLOW)} users")
    print(f"  Time span: {format_timestamp(events[0].timestamp)}"
          f" -> {format_timestamp(events[-1].timestamp)} UTC")

    # Count event types
    type_counts: dict = {}
    for e in events:
        type_counts[e.event_type] = type_counts.get(e.event_type, 0) + 1

    print(f"\n  {c('Event Distribution:', Colors.BOLD)}")
    for etype, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        bar = "#" * (count // 3)
        print(f"    {etype:<22} {count:>4}  {c(bar, Colors.CYAN)}")

    # ── 3. Process Events Through Pipeline ────────────────────────────
    print_section("3. Stream Processing Pipeline Execution")

    start_processing = time.monotonic()
    late_events = 0

    for i, event in enumerate(events):
        process_start = time.monotonic()

        # Process through stream processor
        processor.process_event(event)

        # Route to feature pipeline
        pipeline.process_event(event)

        # Check CEP patterns
        cep.process_event(event)

        # Trigger feature computation periodically
        watermark = processor.current_watermark.timestamp
        if i % 25 == 0 and watermark > 0:
            results = pipeline.trigger(watermark)
            for r in results:
                metrics.record_feature_computed(r.entity_id, r.feature_name)

        # Record metrics
        latency_ms = (time.monotonic() - process_start) * 1000
        metrics.record_event_processed(latency_ms)

        # Progress indicator
        if (i + 1) % 100 == 0:
            elapsed = time.monotonic() - start_processing
            eps = (i + 1) / elapsed
            print(f"  Processed {c(str(i + 1), Colors.GREEN)}/{NUM_EVENTS}"
                  f"  |  {c(f'{eps:.0f}', Colors.YELLOW)} events/s"
                  f"  |  Watermark: {format_timestamp(watermark)}"
                  f"  |  Features: {c(str(len(computed_features_log)), Colors.CYAN)}")

    # Final trigger to flush remaining windows
    final_watermark = time.time() + 600  # Force all windows to close
    final_results = pipeline.trigger(final_watermark)
    for r in final_results:
        metrics.record_feature_computed(r.entity_id, r.feature_name)

    # Check pending absence patterns
    absence_alerts = cep.check_pending_absences(time.time())

    processing_time = time.monotonic() - start_processing

    print(f"\n  {c('Processing Complete!', Colors.BOLD + Colors.GREEN)}")
    print(f"  Total time: {processing_time:.2f}s"
          f"  |  Throughput: {NUM_EVENTS / processing_time:.0f} events/s")

    # ── 4. Computed Features Summary ──────────────────────────────────
    print_section("4. Computed Features Summary")

    # Group features by name
    feature_summary: dict = {}
    for feat in computed_features_log:
        if feat.feature_name not in feature_summary:
            feature_summary[feat.feature_name] = {
                "count": 0, "values": [], "entities": set()
            }
        feature_summary[feat.feature_name]["count"] += 1
        feature_summary[feat.feature_name]["values"].append(feat.feature_value)
        feature_summary[feat.feature_name]["entities"].add(feat.entity_id)

    print(f"  Total feature computations: {c(str(len(computed_features_log)), Colors.YELLOW)}")
    print(f"  Unique feature types: {c(str(len(feature_summary)), Colors.YELLOW)}")
    print()

    header = f"  {'Feature Name':<35} {'Count':>6} {'Users':>6} {'  Avg Value':>12} {'  Max Value':>12}"
    print(c(header, Colors.BOLD))
    print(f"  {'─' * 75}")

    for fname, data in sorted(feature_summary.items()):
        avg_val = sum(data["values"]) / len(data["values"]) if data["values"] else 0
        max_val = max(data["values"]) if data["values"] else 0
        print(f"  {fname:<35} {data['count']:>6} {len(data['entities']):>6}"
              f"  {avg_val:>11.2f} {max_val:>12.2f}")

    # ── 5. Feature Store State ────────────────────────────────────────
    print_section("5. Feature Store - Online Serving Layer")

    entities = store.list_entities()
    print(f"  Entities in store: {c(str(len(entities)), Colors.YELLOW)}")
    print(f"  Total features stored: {c(str(store.get_feature_count()), Colors.YELLOW)}")
    print()

    # Show features for a sample of users
    sample_users = sorted(entities)[:5]
    for user_id in sample_users:
        features = store.get_features(user_id)
        if features:
            print(f"  {c(user_id, Colors.CYAN)}:")
            for fname, fvalue in sorted(features.items()):
                if isinstance(fvalue, float):
                    print(f"    {fname:<35} = {c(f'{fvalue:.2f}', Colors.GREEN)}")
                else:
                    print(f"    {fname:<35} = {c(str(fvalue), Colors.GREEN)}")
            print()

    # ── 6. CEP Alerts ─────────────────────────────────────────────────
    print_section("6. Complex Event Processing - Alerts")

    all_alerts = alerts_log + absence_alerts
    print(f"  Total alerts generated: {c(str(len(all_alerts)), Colors.YELLOW)}")
    print()

    # Group by pattern
    alert_by_pattern: dict = {}
    for alert in all_alerts:
        if alert.pattern_name not in alert_by_pattern:
            alert_by_pattern[alert.pattern_name] = []
        alert_by_pattern[alert.pattern_name].append(alert)

    for pattern_name, pattern_alerts in sorted(alert_by_pattern.items()):
        severity = pattern_alerts[0].severity
        sev_color = Colors.RED if severity == "HIGH" else (
            Colors.YELLOW if severity == "MEDIUM" else Colors.CYAN
        )
        print(f"  {c(f'[{severity}]', sev_color)} {c(pattern_name, Colors.BOLD)}"
              f" ({len(pattern_alerts)} alerts)")

        # Show first 3 alerts
        for alert in pattern_alerts[:3]:
            print(f"    {c('->', Colors.DIM)} Entity: {alert.entity_id}"
                  f"  |  Events: {alert.match.event_count}"
                  f"  |  {alert.alert_id}")

        if len(pattern_alerts) > 3:
            print(f"    {c(f'... and {len(pattern_alerts) - 3} more', Colors.DIM)}")
        print()

    # ── 7. Pipeline Metrics ───────────────────────────────────────────
    print_section("7. Pipeline Metrics Report")

    report = metrics.get_report()

    print(f"  {c('Events:', Colors.BOLD)}")
    print(f"    Processed:      {report['events']['total_processed']:>8}")
    print(f"    Filtered:       {report['events']['filtered']:>8}")
    print(f"    Late:           {report['events']['late']:>8}")
    print(f"    Dropped:        {report['events']['dropped']:>8}")
    print()

    print(f"  {c('Features:', Colors.BOLD)}")
    print(f"    Computed:       {report['features']['total_computed']:>8}")
    print()

    print(f"  {c('Processing Latency:', Colors.BOLD)}")
    lat = report["latency"]["processing"]
    print(f"    Mean:           {lat['mean_ms']:>8.3f} ms")
    print(f"    Median:         {lat['median_ms']:>8.3f} ms")
    print(f"    P95:            {lat['p95_ms']:>8.3f} ms")
    print(f"    P99:            {lat['p99_ms']:>8.3f} ms")
    print(f"    Max:            {lat['max_ms']:>8.3f} ms")
    print()

    print(f"  {c('Stream Processor:', Colors.BOLD)}")
    proc_metrics = processor.metrics
    print(f"    Events:         {proc_metrics['events_processed']:>8}")
    print(f"    Late events:    {proc_metrics['events_late']:>8}")
    print(f"    Watermark lag:  {proc_metrics['watermark_lag_seconds']:>8.1f} s")
    print()

    print(f"  {c('Feature Store:', Colors.BOLD)}")
    store_metrics = store.metrics
    print(f"    Online sets:    {store_metrics['online_sets']:>8}")
    print(f"    Hit rate:       {store_metrics['hit_rate']:>8.1%}")

    # ── 8. Sample Feature Serving ─────────────────────────────────────
    print_section("8. Feature Serving Simulation")

    print(f"  Simulating ML model feature requests...\n")

    serving_times = []
    for user_id in sorted(entities)[:10]:
        serve_start = time.monotonic()
        features = store.get_features(user_id, [
            "page_view_count_5m",
            "total_spend_5m",
            "cart_additions_5m",
            "search_count_5m",
        ])
        serve_time_us = (time.monotonic() - serve_start) * 1_000_000
        serving_times.append(serve_time_us)

        if features:
            feature_str = "  ".join(
                f"{k}={v:.1f}" if isinstance(v, float) else f"{k}={v}"
                for k, v in features.items()
            )
            print(f"  {c(user_id, Colors.CYAN)}"
                  f"  [{c(f'{serve_time_us:.0f}us', Colors.GREEN)}]"
                  f"  {feature_str}")

    if serving_times:
        avg_serve = sum(serving_times) / len(serving_times)
        print(f"\n  Average serving latency: {c(f'{avg_serve:.0f} us', Colors.GREEN + Colors.BOLD)}")

    # ── Done ──────────────────────────────────────────────────────────
    print_section("Pipeline Execution Complete")

    sink.close()

    print(f"  {c('Summary:', Colors.BOLD)}")
    print(f"    Events processed:     {NUM_EVENTS}")
    print(f"    Features computed:    {len(computed_features_log)}")
    print(f"    Alerts generated:     {len(all_alerts)}")
    print(f"    Entities in store:    {len(entities)}")
    print(f"    Processing time:      {processing_time:.2f}s")
    print(f"    Throughput:           {NUM_EVENTS / processing_time:.0f} events/s")
    print()


if __name__ == "__main__":
    main()
