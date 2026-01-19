# metrics_db.py – Final fixed async + PNG export
import asyncio
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from datetime import datetime, timedelta
from dotenv import load_dotenv
from app.config import settings

# ────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────
DB_URL = settings.DATABASE_URL
engine = create_async_engine(DB_URL, echo=False)

# Last 7 days
START_TIME = (datetime.utcnow() - timedelta(days=7)).replace(tzinfo=None)

print(f"Generating visualizations from {START_TIME} onwards...\n")

# ────────────────────────────────────────────────
# Async query(DataFrame)
# ────────────────────────────────────────────────
async def async_read_sql(query: str, params: dict = None) -> pd.DataFrame:
    async with engine.connect() as conn:
        try:
            result = await conn.execute(text(query), params or {})
            rows = result.fetchall()
            if not rows:
                return pd.DataFrame()
            columns = result.keys()
            df = pd.DataFrame(rows, columns=columns)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.date
            return df
        except Exception as e:
            print(f"Query failed: {e}")
            return pd.DataFrame()

# ────────────────────────────────────────────────
# Save figure as PNG
# ────────────────────────────────────────────────
def save_fig(fig, name: str):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{timestamp}.png"
    fig.write_image(filename, scale=2, width=1200, height=800)
    print(f"Saved: {filename}")

# ────────────────────────────────────────────────
# Main async runner
# ────────────────────────────────────────────────
async def main():
    # 1. CPU % per Node
    df_cpu = await async_read_sql("""
        SELECT timestamp, node_id, metric_value
        FROM monitoring_schema.node_metrics
        WHERE metric_type = 'cpu_percent'
          AND timestamp >= :start
        ORDER BY timestamp
    """, params={"start": START_TIME})

    if not df_cpu.empty:
        fig1 = px.line(
            df_cpu,
            x='timestamp',
            y='metric_value',
            color='node_id',
            title='CPU Utilization (%) – Per Node Over Time',
            labels={'metric_value': 'CPU %', 'timestamp': 'Time'},
            template='plotly_dark'
        )
        fig1.update_layout(yaxis_range=[0, 100], hovermode='x unified')
        fig1.show()
        save_fig(fig1, "cpu_per_node")
        print("CPU % per Node saved\n")
    else:
        print("No CPU data found.\n")

    # 2. Memory % Stacked
    df_mem = await async_read_sql("""
        SELECT timestamp, node_id, metric_value
        FROM monitoring_schema.node_metrics
        WHERE metric_type = 'memory_percent'
          AND timestamp >= :start
        ORDER BY timestamp
    """, params={"start": START_TIME})

    if not df_mem.empty:
        fig2 = px.area(
            df_mem,
            x='timestamp',
            y='metric_value',
            color='node_id',
            title='Memory Utilization (%) – Stacked by Node',
            labels={'metric_value': 'Memory %', 'timestamp': 'Time'},
            template='plotly_dark'
        )
        fig2.update_layout(yaxis_range=[0, 100], hovermode='x unified')
        fig2.show()
        save_fig(fig2, "memory_stacked")
        print("Memory % Stacked saved\n")
    else:
        print("No memory data found.\n")

    # 3. System-wide Avg Latency + QoS Bands
    df_latency = await async_read_sql("""
        SELECT timestamp, overall_latency_avg
        FROM monitoring_schema.system_metrics
        WHERE timestamp >= :start
        ORDER BY timestamp
    """, params={"start": START_TIME})

    if not df_latency.empty:
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=df_latency['timestamp'],
            y=df_latency['overall_latency_avg'],
            mode='lines',
            name='Avg Latency (ms)',
            line=dict(color='cyan')
        ))
        fig3.add_hrect(y0=0, y1=150, fillcolor="green", opacity=0.1, line_width=0,
                       annotation_text="Good (Voice)", annotation_position="top left")
        fig3.add_hrect(y0=150, y1=300, fillcolor="yellow", opacity=0.1, line_width=0,
                       annotation_text="Acceptable (Data)", annotation_position="top left")
        fig3.add_hrect(y0=300, y1=1000, fillcolor="red", opacity=0.1, line_width=0,
                       annotation_text="Poor", annotation_position="top left")
        fig3.update_layout(
            title='System-wide Average Latency (ms)',
            xaxis_title='Time',
            yaxis_title='Latency (ms)',
            template='plotly_dark',
            hovermode='x unified'
        )
        fig3.show()
        save_fig(fig3, "system_latency")
        print("System Latency saved\n")
    else:
        print("No latency data found.\n")

    # 4. Active Sessions Trend
    df_sessions = await async_read_sql("""
        SELECT DATE_TRUNC('minute', start_time) as timestamp, COUNT(*) as active_sessions
        FROM edge_schema.sessions
        WHERE status = 'ACTIVE'
          AND start_time >= :start
        GROUP BY timestamp
        ORDER BY timestamp
    """, params={"start": START_TIME})

    if not df_sessions.empty:
        fig4 = px.line(
            df_sessions,
            x='timestamp',
            y='active_sessions',
            title='Active Sessions Over Time (All Edge Nodes)',
            labels={'active_sessions': 'Active Sessions', 'timestamp': 'Time'},
            template='plotly_dark'
        )
        fig4.show()
        save_fig(fig4, "active_sessions")
        print("Active Sessions saved\n")
    else:
        print("No active sessions data found.\n")

    # 5. Migrations & Faults Combined
    df_migrations = await async_read_sql("""
        SELECT DATE(decision_time) as date, COUNT(*) as migration_count
        FROM core_schema.load_decisions
        WHERE decision_time >= :start
        GROUP BY date
        ORDER BY date
    """, params={"start": START_TIME})

    df_faults = await async_read_sql("""
        SELECT DATE(injected_at) as date, COUNT(*) as fault_count,
               AVG(recovery_time_ms) as avg_recovery_ms
        FROM monitoring_schema.fault_logs
        WHERE injected_at >= :start
        GROUP BY date
        ORDER BY date
    """, params={"start": START_TIME})

    # Only merge if both have data and 'date' column
    if not df_migrations.empty and not df_faults.empty and 'date' in df_migrations.columns and 'date' in df_faults.columns:
        df_combined = pd.merge(df_migrations, df_faults, on='date', how='outer').fillna(0)

        fig5 = go.Figure()
        fig5.add_trace(go.Bar(
            x=df_combined['date'],
            y=df_combined['migration_count'],
            name='Migrations',
            marker_color='royalblue'
        ))
        fig5.add_trace(go.Scatter(
            x=df_combined['date'],
            y=df_combined['avg_recovery_ms'],
            name='Avg Recovery Time (ms)',
            yaxis='y2',
            mode='lines+markers',
            line=dict(color='orange', width=3)
        ))
        fig5.update_layout(
            title='Daily Migrations vs Avg Fault Recovery Time',
            xaxis_title='Date',
            yaxis_title='Migration Count',
            yaxis2=dict(title='Recovery Time (ms)', overlaying='y', side='right'),
            template='plotly_dark',
            barmode='group'
        )
        fig5.show()
        save_fig(fig5, "migrations_vs_recovery")
        print("Migrations & Fault Recovery saved\n")
    else:
        print("No migration or fault data to combine.\n")

    print("\nAll done. Check current folder for PNG files.")

# ────────────────────────────────────────────────
# Run
# ────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(main())