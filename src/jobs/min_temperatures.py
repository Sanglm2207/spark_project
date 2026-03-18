"""
Job: Minimum Temperature by Location
Find the minimum temperature (TMIN) recorded at each weather station in 1800.

Run:   python -m src.jobs.min_temperature
Input: data/input/1800.csv
       (station_id, date, measure_type, temperature, ...)
       Temperature stored in tenths of °C, converted to °F for output
"""

from __future__ import annotations

from src.utils import get_spark, get_logger

log = get_logger(__name__)


def parse_line(line: str) -> tuple[str, str, float]:
    """
    Parse a CSV line into (station_id, entry_type, temperature_f).

    Input format: station_id, date, measure_type, temperature, ...
    e.g. "ITE00100554,18000101,TMIN,-148,,,E,"

    Temperature is stored in tenths of °C — converted to °F:
    °F = (tenths_of_C * 0.1) * (9/5) + 32
    """
    fields      = line.split(",")
    station_id  = fields[0]
    entry_type  = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (station_id, entry_type, temperature)


def render_results(results: list[tuple[str, float]]) -> None:
    """Print a sorted table of minimum temperatures per station."""
    if not results:
        log.warn("No data to display")
        return

    print()
    print(f"  {'Station':<20}  {'Min Temp':>10}")
    print(f"  {'─'*20}  {'─'*10}")

    for station, temp_f in sorted(results, key=lambda x: x[1]):
        marker = " 🥶" if temp_f < 32 else ""
        print(f"  {station:<20}  {temp_f:>8.2f}°F{marker}")

    print()


def run() -> None:
    spark = get_spark("MinTemperature")
    sc    = spark.sparkContext
    log.header("Job: Minimum Temperature by Station")

    lines = sc.textFile("data/input/1800.csv")

    # Parse each line into (station_id, entry_type, temperature_f)
    parsed_lines = lines.map(parse_line)

    # Keep only TMIN entries
    min_temps = parsed_lines.filter(lambda x: x[1] == "TMIN")

    # Map to (station_id, temperature) key-value pairs
    station_temps = min_temps.map(lambda x: (x[0], x[2]))

    # reduceByKey: keep the lower temperature for each station
    min_temps_by_station = station_temps.reduceByKey(lambda a, b: min(a, b))

    results = min_temps_by_station.collect()
    log.info(f"Stations found: {len(results)}")
    render_results(results)
    log.ok("Done!")
    spark.stop()


if __name__ == "__main__":
    run()