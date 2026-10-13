"""
Eksperimen perbandingan Pandas dan PySpark pada data tabular.
Hasil eksperimen dibuat oleh program ini; jangan mengisi atau mengubah hasil secara manual.
Jalankan dari lingkungan Python yang sudah memasang dependensi pada requirements.txt.
"""
import os
import time
import platform
import numpy as np
import statistics
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

SIZES = [50_000, 250_000, 1_000_000]
REPEATS = 3
DATA_DIR = "data"
RESULT_DIR = "hasil"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)

def create_dataset(n_rows: int, path: str) -> None:
    """Membuat CSV sintetis deterministik untuk benchmark."""
    if os.path.exists(path):
        return
    categories = ["A", "B", "C", "D", "E"]
    df = pd.DataFrame({
        "id": range(1, n_rows + 1),
        "kategori": [categories[i % len(categories)] for i in range(n_rows)],
        "nilai": [(i * 37 % 1000) / 10 for i in range(n_rows)],
        "kuantitas": [(i * 13 % 20) + 1 for i in range(n_rows)],
    })
    df.to_csv(path, index=False)

def run_pandas(path: str):
    df = pd.read_csv(path)
    filtered = df.loc[df["nilai"] >= 50].copy()
    filtered["nilai_total"] = filtered["nilai"] * filtered["kuantitas"]
    result = (
        filtered.groupby("kategori", as_index=False)["nilai_total"]
        .sum()
        .sort_values("kategori")
        .reset_index(drop=True)
    )
    return result

def run_spark(spark, path: str):
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )
    result = (
        df.filter(F.col("nilai") >= 50)
        .withColumn("nilai_total", F.col("nilai") * F.col("kuantitas"))
        .groupBy("kategori")
        .agg(F.sum("nilai_total").alias("nilai_total"))
        .orderBy("kategori")
    )
    # collect() memaksa eksekusi sehingga waktu yang diukur mencakup pekerjaan Spark.
    rows = result.collect()
    return pd.DataFrame([(r["kategori"], r["nilai_total"]) for r in rows],
                        columns=["kategori", "nilai_total"])

def main():
    print("Python:", platform.python_version())
    print("Pandas:", pd.__version__)
    import pyspark
    print("PySpark:", pyspark.__version__)
    print("Platform:", platform.platform())

    spark = (
        SparkSession.builder
        .appName("Benchmark-Pandas-vs-PySpark")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    records = []

    try:
        for n_rows in SIZES:
            path = os.path.join(DATA_DIR, f"data_{n_rows}.csv")
            create_dataset(n_rows, path)

            # Warm-up Spark dilakukan terpisah dan tidak masuk pengukuran.
            _ = run_spark(spark, path)
            _ = run_pandas(path)

            pandas_result = None
            spark_result = None
            pandas_times, spark_times = [], []

            for run_no in range(1, REPEATS + 1):
                start = time.perf_counter()
                pandas_result = run_pandas(path)
                pandas_times.append(time.perf_counter() - start)

                start = time.perf_counter()
                spark_result = run_spark(spark, path)
                spark_times.append(time.perf_counter() - start)

            
            # Validasi keluaran Pandas dan Spark
            merged = pandas_result.merge(
                spark_result,
                on="kategori",
                suffixes=("_pandas", "_spark"),
                how="outer",
                indicator=True
            )

            # Pastikan kategori yang dihasilkan kedua library sama
            same_categories = (merged["_merge"] == "both").all()

            # Bandingkan hasil numerik dengan toleransi floating point
            if same_categories:
                equivalent = np.isclose(
                    merged["nilai_total_pandas"].to_numpy(dtype=float),
                    merged["nilai_total_spark"].to_numpy(dtype=float),
                    rtol=1e-9,
                    atol=1e-6,
                    equal_nan=False
                ).all()
            else:
                equivalent = False

           
            if not equivalent:
                print("\nPerbedaan hasil ditemukan:")
                print(merged.head(10))
                raise AssertionError(
                    f"Hasil Pandas dan Spark berbeda untuk {n_rows} baris."
                )

            # Simpan waktu pengujian untuk masing-masing pustaka
            for library, times in [
                ("Pandas", pandas_times),
                ("PySpark", spark_times),
            ]:
                records.append({
                    "jumlah_baris": n_rows,
                    "pustaka": library,
                    "pengulangan": REPEATS,
                    "waktu_rata_rata_detik": statistics.mean(times),
                    "waktu_minimum_detik": min(times),
                    "waktu_maksimum_detik": max(times),
                    "hasil_setara": equivalent,
                })

            print(f"Selesai {n_rows:,} baris | hasil setara: {equivalent}")
    finally:
        spark.stop()

    print("Jumlah records:", len(records))
    print("Isi records:", records)  

    results = pd.DataFrame(records)
    results.to_csv(os.path.join(RESULT_DIR, "hasil_benchmark.csv"), index=False)


    print("Kolom results:", results.columns.tolist())
    print(results.head())

    pivot = results.pivot(
        index="jumlah_baris",
        columns="pustaka",
        values="waktu_rata_rata_detik"
    )

    ax = pivot.plot(kind="bar", figsize=(9, 5))
    ax.set_title("Waktu Eksekusi Rata-rata: Pandas vs PySpark")
    ax.set_xlabel("Jumlah baris")
    ax.set_ylabel("Waktu (detik)")
    ax.tick_params(axis="x", rotation=0)
    ax.grid(axis="y", alpha=0.25)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULT_DIR, "grafik_waktu.png"), dpi=160)
    plt.close()

    print("\nHasil benchmark:")
    print(results.to_string(index=False))
    print("\nFile tersimpan di folder 'hasil'. Gunakan angka ini dalam laporan.")

if __name__ == "__main__":
    main()
