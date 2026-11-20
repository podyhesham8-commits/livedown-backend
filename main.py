from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import yt_dlp
import os
import asyncio
import json
import threading
import re
import uuid
import subprocess

app = FastAPI(title="livedown Downloader")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

jobs = {}
latest_job_id: Optional[str] = None
MAX_STORED_JOBS = 30

def get_ffmpeg_path() -> Optional[str]:
    local_ffmpeg = os.path.join(os.getcwd(), "ffmpeg.exe")
    if os.path.exists(local_ffmpeg):
        return local_ffmpeg
    return None

def clean_ansi(text: str) -> str:
    return re.sub(r'\x1b\[[0-9;]*m', '', text or '').strip()

def parse_percent(raw_percent: str) -> float:
    cleaned = clean_ansi(raw_percent).replace('%', '').strip()
    try:
        val = float(cleaned)
        return max(0.0, min(100.0, val))
    except ValueError:
        return 0.0

def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', '_', name or 'media').strip()
    return cleaned[:80] if cleaned else 'media'

def cleanup_old_jobs():
    if len(jobs) <= MAX_STORED_JOBS:
        return
    keys_to_remove = list(jobs.keys())[:-MAX_STORED_JOBS]
    for k in keys_to_remove:
        old_job = jobs.pop(k, None)
        if old_job and old_job.get("file_path") and os.path.exists(old_job["file_path"]):
            try:
                os.remove(old_job["file_path"])
            except OSError:
                pass

@app.post("/api/info")
async def get_video_info(data: dict):
    url = (data or {}).get("url")
    if not url:
        raise HTTPException(status_code=400, detail="الرابط مطلوب")

    is_playlist = "playlist" in url or "list=" in url

    ydl_opts = {
        'extract_flat': 'in_playlist' if is_playlist else False,
        'noplaylist': False if is_playlist else True,
        'quiet': True,
        'no_warnings': True,
    }

    qualities = [
        {"id": "best", "label": "أعلى جودة متاحة (فيديو + صوت)"},
        {"id": "1080", "label": "1080p Full HD (فيديو + صوت)"},
        {"id": "720", "label": "720p HD (فيديو + صوت)"},
        {"id": "480", "label": "480p (فيديو + صوت)"},
        {"id": "360", "label": "360p (فيديو + صوت)"},
        {"id": "240", "label": "240p (فيديو + صوت)"},
        {"id": "144", "label": "144p (فيديو + صوت)"},
        {"id": "mp3", "label": "MP3 صوت فقط (أعلى جودة صوت)"},
    ]

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

            if 'entries' in info:
                videos = []
                for entry in info['entries']:
                    if entry:
                        vid_id = entry.get('id')
                        vid_url = entry.get('url')
                        if vid_url and not str(vid_url).startswith("http"):
                            vid_url = f"https://www.youtube.com/watch?v={vid_id or vid_url}"
                        elif not vid_url and vid_id:
                            vid_url = f"https://www.youtube.com/watch?v={vid_id}"

                        thumb = entry.get('thumbnail')
                        if not thumb and vid_id:
                            thumb = f"https://i.ytimg.com/vi/{vid_id}/hqdefault.jpg"

                        videos.append({
                            "title": entry.get('title', 'فيديو بدون عنوان'),
                            "url": vid_url,
                            "thumbnail": thumb or "",
                        })
                return {
                    "type": "playlist",
                    "title": info.get('title', 'قائمة تشغيل'),
                    "videos": videos,
                    "qualities": qualities
                }

            return {
                "type": "single",
                "title": info.get('title', 'media'),
                "thumbnail": info.get('thumbnail') or "",
                "duration": info.get('duration'),
                "original_url": url,
                "qualities": qualities
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في جلب البيانات: {str(e)}")

def build_format_selector(quality: str) -> str:
    if quality == "mp3":
        return "bestaudio/best"
    if quality == "best":
        return "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best"
    try:
        h = int(quality)
        return (
            f"bestvideo[height<={h}][ext=mp4]+bestaudio[ext=m4a]/"
            f"bestvideo[height<={h}]+bestaudio/"
            f"best[height<={h}][ext=mp4]/"
            f"best[height<={h}]/"
            f"best"
        )
    except ValueError:
        return "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best"

@app.get("/api/start-download")
async def start_download(url: str, quality: str = "best", title: str = "media"):
    global latest_job_id
    cleanup_old_jobs()

    job_id = uuid.uuid4().hex[:10]
    latest_job_id = job_id

    is_mp3 = (quality == "mp3")
    ext = "mp3" if is_mp3 else "mp4"
    media_type = "audio/mpeg" if is_mp3 else "video/mp4"
    safe_title = sanitize_filename(title)

    jobs[job_id] = {
        "percent": "0%",
        "numeric_percent": 0.0,
        "speed": "جاري البدء...",
        "eta": "--:--",
        "status": "downloading",
        "filename": f"{safe_title}.{ext}",
        "media_type": media_type,
        "file_path": None,
        "error": None
    }

    def progress_hook(d):
        if d['status'] == 'downloading':
            clean_pct = clean_ansi(d.get('_percent_str', '0%'))
            num_pct = parse_percent(clean_pct)
            jobs[job_id]["percent"] = clean_pct if clean_pct else f"{num_pct:.1f}%"
            jobs[job_id]["numeric_percent"] = num_pct
            jobs[job_id]["speed"] = clean_ansi(d.get('_speed_str', '0 KB/s'))
            jobs[job_id]["eta"] = clean_ansi(d.get('_eta_str', '--:--'))
            jobs[job_id]["status"] = "downloading"
        elif d['status'] == 'postprocessing':
            jobs[job_id]["percent"] = "99%"
            jobs[job_id]["numeric_percent"] = 99.0
            jobs[job_id]["eta"] = "دمج/تحويل..."
            jobs[job_id]["status"] = "processing"
        elif d['status'] == 'finished':
            jobs[job_id]["percent"] = "100%"
            jobs[job_id]["numeric_percent"] = 100.0
            jobs[job_id]["eta"] = "00:00"

    ffmpeg_path = get_ffmpeg_path()
    base_prefix = f"livedown_{job_id}"

    def run_download():
        try:
            ydl_opts = {
                'format': build_format_selector(quality),
                'outtmpl': os.path.join(DOWNLOAD_DIR, f"{base_prefix}.%(ext)s"),
                'progress_hooks': [progress_hook],
                'noplaylist': True,
                'quiet': True,
                'no_warnings': True,
                'extractor_args': {'youtube': {'player_client': ['android', 'web']}},
            }

            if ffmpeg_path:
                ydl_opts['ffmpeg_location'] = ffmpeg_path

            if is_mp3:
                ydl_opts['postprocessors'] = [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }]
            else:
                ydl_opts['merge_output_format'] = 'mp4'

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

            final_file = os.path.join(DOWNLOAD_DIR, f"{base_prefix}.{ext}")
            if not os.path.exists(final_file):
                for f in os.listdir(DOWNLOAD_DIR):
                    if f.startswith(base_prefix):
                        src = os.path.join(DOWNLOAD_DIR, f)
                        if not is_mp3 and ffmpeg_path:
                            subprocess.run(
                                [ffmpeg_path, "-y", "-i", src, "-c:v", "copy", "-c:a", "aac", final_file],
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                check=False
                            )
                        if not os.path.exists(final_file):
                            os.rename(src, final_file)
                        break

            if not os.path.exists(final_file):
                raise FileNotFoundError("لم يتم العثور على الملف النهائي بعد المعالجة")

            jobs[job_id]["file_path"] = final_file
            jobs[job_id]["percent"] = "100%"
            jobs[job_id]["numeric_percent"] = 100.0
            jobs[job_id]["status"] = "finished"

        except Exception as e:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(e)

    threading.Thread(target=run_download, daemon=True).start()
    return {"job_id": job_id}

@app.get("/api/progress-stream")
async def progress_stream(job_id: Optional[str] = Query(default=None)):
    target_id = job_id or latest_job_id
    if not target_id or target_id not in jobs:
        async def empty_stream():
            yield f"data: {json.dumps({'percent': '0%', 'numeric_percent': 0, 'speed': '--', 'eta': '--:--', 'status': 'idle'})}\n\n"
        return StreamingResponse(empty_stream(), media_type="text/event-stream")

    async def event_generator():
        while True:
            current = jobs.get(target_id)
            if not current:
                break
            yield f"data: {json.dumps(current)}\n\n"
            if current["status"] in ("finished", "error"):
                break
            await asyncio.sleep(0.25)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/api/get-file")
async def get_file(job_id: Optional[str] = Query(default=None)):
    target_id = job_id or latest_job_id
    job = jobs.get(target_id) if target_id else None
    if not job or not job.get("file_path") or not os.path.exists(job["file_path"]):
        raise HTTPException(status_code=404, detail="الملف غير جاهز بعد")

    return FileResponse(
        job["file_path"],
        media_type=job.get("media_type", "video/mp4"),
        filename=job.get("filename", "livedown_media.mp4")
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    is_prod = bool(os.environ.get("PORT") or os.environ.get("RENDER") or os.environ.get("RAILWAY_ENVIRONMENT"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=not is_prod)