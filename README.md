# Eksplorasi Mahadata: Perbandingan Pandas dan PySpark

Repository ini berisi kode program dan rancangan eksperimen untuk membandingkan waktu pemrosesan data menggunakan Pandas dan PySpark. Pengujian dilakukan pada satu laptop dengan menjalankan Apache Spark dalam mode lokal.

## Isi Repository

- `benchmark.py`: Script utama untuk membuat dataset sintetis, menjalankan pengujian, membandingkan hasil pemrosesan, serta menghasilkan file CSV dan grafik.
- `requirements.txt`: Daftar pustaka Python beserta versi yang digunakan dalam eksperimen.
- `Laporan_Eksplorasi_Mahadata.pdf`: Draf laporan eksperimen yang perlu dilengkapi menggunakan hasil pengujian sebenarnya.

## Persiapan Lingkungan

Sebelum menjalankan program, pastikan perangkat telah memenuhi kebutuhan berikut:

- Python dengan versi yang sesuai dengan pustaka pada `requirements.txt`.
- Java JDK 17 dengan variabel lingkungan `JAVA_HOME` dan `PATH` yang telah dikonfigurasi.
- Windows 10 atau Windows 11.
- Visual Studio Code sebagai editor kode (opsional).

## Cara Menjalankan Program

Buka PowerShell pada folder `kode`, kemudian jalankan perintah berikut secara berurutan:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python benchmark.py
```

Apabila aktivasi virtual environment ditolak oleh PowerShell, gunakan Command Prompt atau sesuaikan pengaturan eksekusi skrip sesuai kebutuhan. Jika Java belum terdeteksi, periksa hasil perintah `java -version` dan pastikan konfigurasi `JAVA_HOME` serta `PATH` sudah mengarah ke instalasi JDK yang benar.

## Hasil Pengujian

Setelah program selesai dijalankan, akan terbentuk folder `data` dan `hasil`. Folder `hasil` digunakan untuk menyimpan keluaran utama eksperimen, yaitu:

- `hasil/hasil_benchmark.csv`: Berisi hasil pengukuran waktu eksekusi Pandas dan PySpark.
- `hasil/grafik_waktu.png`: Grafik perbandingan waktu pemrosesan kedua pustaka.

Pengukuran waktu mencakup pembacaan file CSV, penyaringan data, transformasi, agregasi, dan pengambilan hasil ke driver. Waktu pembuatan SparkSession dan proses *warm-up* tidak dihitung dalam pengukuran. Untuk menjaga konsistensi pengujian, gunakan perangkat yang sama, tutup aplikasi yang membebani sistem, dan pertahankan ukuran dataset sesuai rancangan eksperimen.

## Ketentuan Pelaporan Hasil

Hasil eksperimen harus berasal dari program yang benar-benar dijalankan, bukan dari perkiraan. Simpan file keluaran yang dihasilkan dan catat spesifikasi laptop, versi Python, Java, serta pustaka yang digunakan. Kendala selama instalasi maupun pengujian juga perlu didokumentasikan. Perbedaan perangkat dan konfigurasi dapat menyebabkan hasil pengukuran yang berbeda.

## Artikel Referensi

Mekouar, Y., Lahmer, M., & Karim, M. (2025). *Optimizing Data Pipelines for Green AI: A Comparative Analysis of Pandas, Polars, and PySpark for CO₂ Emission Prediction*. Computers, 14(8), 319. https://doi.org/10.3390/computers14080319

Eksperimen dalam repository ini berfokus pada perbandingan Pandas dan PySpark pada lingkungan lokal. Pengujian tersebut merupakan pengembangan dari topik artikel referensi dan tidak dimaksudkan sebagai reproduksi penuh terhadap seluruh metode maupun hasil penelitian aslinya.