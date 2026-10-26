# WAN Batch Studio

A local Windows queue manager for sequential ComfyUI image-to-video workflows. Build a chain of segments using the previous segment's lossless final frame, then optionally join the clips.

**[Windows installer](https://github.com/mysticalg/WAN-Batch-Studio/releases/latest)** | **[Website](https://mysticalg.github.io/WAN-Batch-Studio/)**

## Install

1. Download and run the Windows x64 installer. Python, Tk, NumPy, Pillow, timezone data and websocket-client are included.
2. Install **FFmpeg and ffprobe** on PATH if needed: `winget install --id Gyan.FFmpeg -e`. Reopen the app afterward. FFmpeg is **not bundled**.
3. Start your own **ComfyUI** server with your selected models. ComfyUI and models are **not included**.
4. Export your workflow with ComfyUI's **Save (API Format)** and select it in the app. Set the server URL (default `http://127.0.0.1:8000`).
5. Set the segment count, edit main positive prompt and segment additions, add images, then click **Start / Resume**.

Windows 10/11 x64. The installer is unsigned; Windows may show an unknown-publisher prompt. Verify its checksum against `SHA256SUMS.txt` from the same release.

## Workflow requirements

One `LoadImage` and one `SaveVideo` or `VHS_VideoCombine` node. For lossless continuation and colour matching, use `SaveVideo` directly connected to `CreateVideo`, or a `VHS_VideoCombine` IMAGE input, with a ComfyUI version supporting `ImageFromBatch` index `-1`. The app branches this same pre-encoding batch to `SaveImage`; missing expected raw outputs stop safely. Legacy/custom graphs retain decoded-video compatibility. Visual graph JSON is not supported: export API format.

Bring your own workflow and models. No personal workflows, model weights, saved prompts, queues, images or generated media are distributed.

## Features

- Sequential predecessor-frame chains; optional joining with individual segments retained.
- Editable main positive prompt and future segment descriptions; submitted prompts stay fixed.
- Lossless pre-encoding continuation PNGs for supported graphs.
- Optional bounded RGB colour matching applied to video and continuation PNG, preserving originals. This reduces small global tone shifts; it does not guarantee sharpness or eliminate model/VAE reconstruction drift.
- Pause after current, offline Cancel / Reset, idle-only Clear Queue and ownership-scoped cancellation. Unrelated jobs are preserved.
- Flushed atomic queue writes, two validated backups, corruption recovery, local logs and render timers.

## Files and recovery

Installed user data lives in `%LOCALAPPDATA%\WANBatchStudio`, outside the application folder. Source runs use their source folder. In your chosen output folder, segment videos stay at the top level, frame PNGs go in `frames/`, and final joined clips go in `joined/`. Existing records retain their saved paths.

Uninstall removes the application, not user data or generated media. Back up the data/output folders yourself for extra protection. A queue can contain full private prompts and local paths: do not upload it publicly.

Uncertain submissions are reconciled by prompt identity, never blindly repeated. After a backend crash use **Cancel / Reset stuck job**, then review before resuming. **Clear Queue** archives records only when the worker has stopped and ComfyUI reports idle; it never deletes media. Running prompts can only be cancelled when verified prompt-specific interruption is supported.

## Build from source

```powershell
python -m pip install -r requirements-build.txt
python verify.py
python -m py_compile app.py
.\scripts\build.ps1
```

Needs Windows x64, Python 3.14 with Tk, Inno Setup 6+, and FFmpeg/ffprobe for synthetic-media tests. Output installer and checksum are written to `release/`. GitHub Actions builds release assets for version tags.

`WAN_BATCH_STUDIO_DATA` overrides the data directory for isolated testing. `WANBatchStudio.exe --self-test-report report.json` checks the frozen libraries and a real Tk window without contacting ComfyUI or generating media.

## Validation

Tests use synthetic media and fake ComfyUI transport. They cover chaining, local processing, prompt editing, recovery and output paths; they do not prove a particular model/workflow's visual quality. Installer smoke tests are separate.

## License

MIT for WAN Batch Studio. Bundled dependencies retain their own licenses; notices are installed under `third-party/`. FFmpeg, ComfyUI and model weights are separate projects with separate licenses.
