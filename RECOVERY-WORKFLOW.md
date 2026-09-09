# Wordavee Resilient Workflow

Workflow baru: `.github/workflows/wordivee-buffer-resilient.yml`.

## Pengaman produksi

- `WORDIVEE_AUTOMATION_ENABLED` harus tetap `false` sampai seluruh tes selesai.
- Workflow lama tidak dihapus. Backup disimpan di `.github/workflow-backups/` sehingga tidak dijalankan GitHub Actions.
- Generator, secrets, channel Buffer `wordavee`, `wordavee/videos`, dan `wordavee/audio` tidak diubah.
- Cleanup Cloudinary sengaja ditahan. Sistem belum menghapus aset apa pun karena Buffer GraphQL yang digunakan belum memberikan hubungan aset yang cukup untuk menjamin cleanup aman.

## Cara kerja

1. Validasi Cloudinary, Buffer, channel TikTok, tanggal, slot, dan Quote Guard.
2. Simpan intent deterministik ke `state/YYYY-MM-DD.json` sebelum membuat post.
3. Baca queue Buffer tanpa mensyaratkan queue kosong.
4. Gunakan kombinasi tanggal + slot + dueAt sebagai kunci idempotensi.
5. Jika dueAt sudah ada di Buffer, slot direkonsiliasi dan dilewati.
6. Jika satu slot gagal, slot berikutnya tetap diproses.
7. Error sementara disimpan sebagai `failed_retryable`; recovery hanya mencoba slot yang belum sukses.
8. Quote masuk `quotes-history.json` hanya setelah Buffer menerima post atau post yang sama berhasil direkonsiliasi.

## Retry dan klasifikasi error

- Retry/backoff: 20, 60, lalu 120 detik untuk timeout, rate limit, dan error server sementara.
- Tidak retry: 401/403, input tidak valid, channel salah/terputus, serta aset yang benar-benar tidak ditemukan.
- Credential Cloudinary/Buffer invalid dan channel TikTok tidak ditemukan membuat job gagal.
- Kegagalan satu slot menjadi warning pada summary dan tidak membuat job gagal total.

## Recovery manual

Actions → **Wordivee Buffer Resilient** → **Run workflow**:

- Pilih tanggal dan sesi.
- Isi `slots` untuk mencoba slot tertentu, misalnya `03` atau `03,05`.
- Pilih `recovery`.
- Biarkan `dry_run=true` untuk pemeriksaan aman.
- Gunakan `dry_run=false` hanya setelah hasil dry run benar.

Recovery otomatis berjalan 15 menit setelah setiap sesi utama: 00:45, 10:45, dan 20:45 WIB.

## Checklist sebelum produksi

- [ ] Offline resilience tests lulus.
- [ ] Dry run sesi 1, 2, dan 3 lulus pada workflow baru.
- [ ] Fault isolation membuktikan slot sesudah slot gagal tetap diproses.
- [ ] Rerun/idempotency tidak membuat post kedua.
- [ ] Recovery hanya memproses slot `failed_retryable` atau `planned`.
- [ ] Channel yang terdeteksi adalah TikTok `wordavee`.
- [ ] Semua 24 export untuk tanggal produksi tersedia dan Quote Guard lulus.
- [ ] `WORDIVEE_AUTOMATION_ENABLED` baru diubah menjadi `true` secara manual setelah semua poin di atas lulus.
