#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import NoTranscriptFound, TranscriptsDisabled, VideoUnavailable


USER_AGENT = "Mozilla/5.0 (compatible; youtube-channel-ingest/2.0)"


@dataclass
class ChannelContext:
    channel_id: str
    channel_label: str
    api_key: str
    client_version: str
    initial_data: dict[str, Any]


@dataclass
class VideoEntry:
    video_id: str
    title: str
    published_text: str
    published_at: datetime | None
    link: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Baixa raw transcripts públicos de vídeos de um canal do YouTube."
    )
    parser.add_argument("channel", help="URL do canal, handle (@canal), ou channel id (UC...).")
    parser.add_argument("--output-root", default="IDEIAS/youtube", help="Diretório raiz da saída.")
    parser.add_argument("--months", type=int, default=12, help="Considera vídeos dos últimos N meses.")
    parser.add_argument("--languages", default="pt,pt-BR,en,en-US", help="Prioridade de idiomas.")
    parser.add_argument("--translate-to", default="", help="Traduz transcript, se suportado.")
    parser.add_argument("--limit", type=int, default=0, help="Máximo de vídeos após o filtro.")
    parser.add_argument("--preserve-formatting", action="store_true", help="Preserva formatação do transcript.")
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.75,
        help="Pausa entre tentativas de transcript para reduzir bloqueios.",
    )
    parser.add_argument(
        "--pause-jitter",
        type=float,
        default=0.5,
        help="Jitter aleatorio somado ao pause-seconds.",
    )
    parser.add_argument(
        "--block-threshold",
        type=int,
        default=3,
        help="Interrompe a rodada apos N bloqueios consecutivos do YouTube.",
    )
    parser.add_argument(
        "--block-retries",
        type=int,
        default=2,
        help="Numero de retries com backoff ao detectar IpBlocked/RequestBlocked.",
    )
    parser.add_argument(
        "--block-backoff-base",
        type=float,
        default=20.0,
        help="Backoff base em segundos para bloqueio de IP.",
    )
    return parser.parse_args()


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "canal"


def fetch_text(url: str) -> str:
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    response.raise_for_status()
    return response.text


def parse_published_text(text: str, now: datetime) -> datetime | None:
    normalized = text.strip().lower()
    match = re.search(r"(\d+)\s+(hour|day|week|month|year)s?\s+ago", normalized)
    if not match:
        return None

    amount = int(match.group(1))
    unit = match.group(2)
    if unit == "hour":
        return now - timedelta(hours=amount)
    if unit == "day":
        return now - timedelta(days=amount)
    if unit == "week":
        return now - timedelta(weeks=amount)
    if unit == "month":
        return now - timedelta(days=amount * 30)
    if unit == "year":
        return now - timedelta(days=amount * 365)
    return None


def extract_text(node: dict[str, Any] | None) -> str:
    if not node:
        return ""
    runs = node.get("runs") or []
    if runs:
        return "".join(str(item.get("text", "")) for item in runs).strip()
    return str(node.get("simpleText", "")).strip()


def find_first(node: Any, key: str) -> Any:
    if isinstance(node, dict):
        if key in node:
            return node[key]
        for value in node.values():
            result = find_first(value, key)
            if result is not None:
                return result
    elif isinstance(node, list):
        for value in node:
            result = find_first(value, key)
            if result is not None:
                return result
    return None


def resolve_channel_url(channel: str) -> str:
    channel = channel.strip()
    if channel.startswith("@"):
        return f"https://www.youtube.com/{channel}/videos"
    if re.fullmatch(r"UC[\w-]{20,}", channel):
        return f"https://www.youtube.com/channel/{channel}/videos"

    parsed = urlparse(channel)
    if not parsed.scheme:
        channel = f"https://{channel}"
        parsed = urlparse(channel)

    path = parsed.path.rstrip("/")
    if path.endswith("/videos"):
        return channel
    return f"{channel.rstrip('/')}/videos"


def load_channel_context(channel: str) -> ChannelContext:
    channel_url = resolve_channel_url(channel)
    html = fetch_text(channel_url)

    initial_match = re.search(r"var ytInitialData = (\{.*?\});</script>", html)
    api_key_match = re.search(r'INNERTUBE_API_KEY":"([^"]+)', html)
    client_version_match = re.search(r'INNERTUBE_CLIENT_VERSION":"([^"]+)', html)
    channel_id_match = re.search(r'"browseId":"(UC[\w-]+)"', html)
    canonical_match = re.search(r'<link rel="canonical" href="https://www\.youtube\.com/(@[^"]+)"', html)

    if not initial_match or not api_key_match or not client_version_match or not channel_id_match:
        raise RuntimeError(f"Nao consegui carregar o contexto do canal: {channel_url}")

    initial_data = json.loads(initial_match.group(1))
    channel_label = canonical_match.group(1) if canonical_match else channel_id_match.group(1)
    return ChannelContext(
        channel_id=channel_id_match.group(1),
        channel_label=channel_label,
        api_key=api_key_match.group(1),
        client_version=client_version_match.group(1),
        initial_data=initial_data,
    )


def extract_videos_tab(initial_data: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    tabs = initial_data["contents"]["twoColumnBrowseResultsRenderer"]["tabs"]
    for tab in tabs:
        renderer = tab.get("tabRenderer")
        if renderer and renderer.get("title") == "Videos":
            rich = renderer["content"]["richGridRenderer"]
            items = rich["contents"]
            continuation = None
            if items and "continuationItemRenderer" in items[-1]:
                continuation = items[-1]["continuationItemRenderer"]["continuationEndpoint"]["continuationCommand"]["token"]
            return items, continuation
    raise RuntimeError("Nao encontrei a aba Videos no ytInitialData.")


def items_to_videos(items: list[dict[str, Any]], now: datetime) -> list[VideoEntry]:
    videos: list[VideoEntry] = []
    for item in items:
        rich = item.get("richItemRenderer")
        if not rich:
            continue
        video = rich.get("content", {}).get("videoRenderer")
        if not video:
            continue
        video_id = video.get("videoId", "").strip()
        title = extract_text(video.get("title"))
        published_text = extract_text(video.get("publishedTimeText"))
        if not video_id or not title:
            continue
        videos.append(
            VideoEntry(
                video_id=video_id,
                title=title,
                published_text=published_text,
                published_at=parse_published_text(published_text, now),
                link=f"https://www.youtube.com/watch?v={video_id}",
            )
        )
    return videos


def fetch_continuation(context: ChannelContext, token: str) -> tuple[list[dict[str, Any]], str | None]:
    payload = {
        "context": {"client": {"clientName": "WEB", "clientVersion": context.client_version}},
        "continuation": token,
    }
    response = requests.post(
        f"https://www.youtube.com/youtubei/v1/browse?key={context.api_key}",
        json=payload,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    actions = data.get("onResponseReceivedActions") or []
    if not actions:
        return [], None
    action = actions[0]
    append = action.get("appendContinuationItemsAction") or action.get("reloadContinuationItemsCommand") or {}
    items = append.get("continuationItems") or []
    next_token = None
    if items and "continuationItemRenderer" in items[-1]:
        next_token = items[-1]["continuationItemRenderer"]["continuationEndpoint"]["continuationCommand"]["token"]
    return items, next_token


def collect_channel_videos(context: ChannelContext, months: int, limit: int) -> list[VideoEntry]:
    now = datetime.now(UTC)
    since = now - timedelta(days=max(months, 0) * 30)
    initial_items, continuation = extract_videos_tab(context.initial_data)
    videos = items_to_videos(initial_items, now)
    seen_ids = {video.video_id for video in videos}

    while continuation:
        oldest = videos[-1].published_at if videos else None
        if oldest and oldest < since:
            break
        if limit > 0 and len(videos) >= limit:
            break

        items, continuation = fetch_continuation(context, continuation)
        page_videos = items_to_videos(items, now)
        for video in page_videos:
            if video.video_id in seen_ids:
                continue
            seen_ids.add(video.video_id)
            videos.append(video)

    filtered = [video for video in videos if video.published_at is None or video.published_at >= since]
    filtered.sort(key=lambda item: item.published_at or datetime.min.replace(tzinfo=UTC), reverse=True)
    if limit > 0:
        filtered = filtered[:limit]
    return filtered


def pick_transcript(
    api: YouTubeTranscriptApi,
    video_id: str,
    languages: list[str],
    translate_to: str,
    preserve_formatting: bool,
):
    transcript_list = api.list(video_id)

    try:
        selected = transcript_list.find_manually_created_transcript(languages)
    except NoTranscriptFound:
        try:
            selected = transcript_list.find_generated_transcript(languages)
        except NoTranscriptFound:
            try:
                selected = transcript_list.find_transcript(languages)
            except NoTranscriptFound:
                selected = None

    if selected is None:
        available = list(transcript_list)
        if not available:
            raise NoTranscriptFound(video_id, languages, transcript_list)
        selected = available[0]
        if translate_to and selected.is_translatable:
            selected = selected.translate(translate_to)

    fetched = selected.fetch(preserve_formatting=preserve_formatting)
    return selected, fetched


def transcript_to_text(snippets: list[dict[str, Any]]) -> str:
    return "\n".join(" ".join(str(snippet["text"]).split()) for snippet in snippets if snippet.get("text")).strip()


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_existing_transcripts(videos_root: Path) -> dict[str, Path]:
    existing: dict[str, Path] = {}
    if not videos_root.exists():
        return existing
    for metadata_path in videos_root.glob("*/metadata.json"):
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        video_id = str(payload.get("video_id", "")).strip()
        transcript_path = metadata_path.parent / "transcript.txt"
        if video_id and transcript_path.exists():
            existing[video_id] = metadata_path.parent
    return existing


def build_video_dir(videos_root: Path, video: VideoEntry) -> Path:
    date_prefix = (video.published_at or datetime.now(UTC)).strftime("%Y-%m-%d")
    return videos_root / f"{date_prefix}--{slugify(video.title)}--{video.video_id}"


def is_block_error(exc: Exception) -> bool:
    return type(exc).__name__ in {"IpBlocked", "RequestBlocked"}


def sleep_with_jitter(base_seconds: float, jitter_seconds: float) -> None:
    duration = max(0.0, base_seconds)
    if jitter_seconds > 0:
        duration += random.uniform(0, jitter_seconds)
    if duration > 0:
        time.sleep(duration)


def fetch_transcript_with_retries(
    *,
    api: YouTubeTranscriptApi,
    video_id: str,
    languages: list[str],
    translate_to: str,
    preserve_formatting: bool,
    retries: int,
    backoff_base: float,
):
    attempt = 0
    while True:
        try:
            return pick_transcript(
                api=api,
                video_id=video_id,
                languages=languages,
                translate_to=translate_to,
                preserve_formatting=preserve_formatting,
            )
        except Exception as exc:
            if not is_block_error(exc) or attempt >= retries:
                raise
            sleep_for = backoff_base * (2**attempt) + random.uniform(0, 5)
            print(
                f"Bloqueio detectado em {video_id}. Backoff de {sleep_for:.1f}s antes do retry {attempt + 1}.",
                file=sys.stderr,
            )
            time.sleep(sleep_for)
            attempt += 1


def write_checklist(channel_root: Path, videos: list[dict[str, Any]]) -> None:
    present = sum(1 for video in videos if video["transcript_status"] == "present")
    missing = sum(1 for video in videos if video["transcript_status"] == "missing")
    pending = sum(1 for video in videos if video["transcript_status"] == "pending")
    lines = [
        "# Raw Transcripts Checklist",
        "",
        "Status atual:",
        "",
        f"- Total de videos no recorte: {len(videos)}",
        f"- Raw transcripts presentes: {present}",
        f"- Raw transcripts faltando: {missing}",
        f"- Raw transcripts pendentes: {pending}",
        "",
        "Checklist:",
        "",
    ]
    for video in videos:
        marker = "x" if video["transcript_status"] == "present" else " "
        lines.append(f"- [{marker}] {video['published_text']} | {video['title']}")
    (channel_root / "RAW_TRANSCRIPTS_CHECKLIST.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    languages = [item.strip() for item in args.languages.split(",") if item.strip()]
    context = load_channel_context(args.channel)
    channel_slug = slugify(context.channel_label.replace("@", ""))

    output_root = Path(args.output_root).resolve()
    channel_root = output_root / channel_slug
    videos_root = channel_root / "videos"
    videos_root.mkdir(parents=True, exist_ok=True)

    all_videos = collect_channel_videos(context=context, months=args.months, limit=args.limit)
    existing = load_existing_transcripts(videos_root)

    api = YouTubeTranscriptApi()
    inventory: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    consecutive_block_errors = 0
    halted_due_to_block = False

    for position, video in enumerate(all_videos, start=1):
        metadata = {
            "video_id": video.video_id,
            "title": video.title,
            "published_text": video.published_text,
            "published_at": video.published_at.isoformat() if video.published_at else None,
            "url": video.link,
            "channel_id": context.channel_id,
            "channel_label": context.channel_label,
        }

        if video.video_id in existing:
            inventory.append(
                {
                    **metadata,
                    "transcript_status": "present",
                    "path": str(existing[video.video_id]),
                }
            )
            print(f"[{position}/{len(all_videos)}] SKIP {video.title}")
            consecutive_block_errors = 0
            continue

        video_dir = build_video_dir(videos_root, video)
        video_dir.mkdir(parents=True, exist_ok=True)
        write_json(video_dir / "metadata.json", metadata)

        try:
            selected, fetched = fetch_transcript_with_retries(
                api=api,
                video_id=video.video_id,
                languages=languages,
                translate_to=args.translate_to.strip(),
                preserve_formatting=args.preserve_formatting,
                retries=args.block_retries,
                backoff_base=args.block_backoff_base,
            )
            transcript_raw = fetched.to_raw_data()
            transcript_text = transcript_to_text(transcript_raw)
            write_json(
                video_dir / "transcript.json",
                {
                    "video_id": video.video_id,
                    "language": fetched.language,
                    "language_code": fetched.language_code,
                    "is_generated": fetched.is_generated,
                    "segments": transcript_raw,
                },
            )
            (video_dir / "transcript.txt").write_text(transcript_text, encoding="utf-8")
            inventory.append(
                {
                    **metadata,
                    "transcript_status": "present",
                    "language": fetched.language,
                    "language_code": fetched.language_code,
                    "is_generated": fetched.is_generated,
                    "path": str(video_dir),
                }
            )
            print(
                f"[{position}/{len(all_videos)}] OK {video.title} "
                f"({fetched.language_code}, generated={fetched.is_generated})"
            )
            consecutive_block_errors = 0
        except (NoTranscriptFound, TranscriptsDisabled, VideoUnavailable, requests.RequestException, Exception) as exc:
            if is_block_error(exc):
                consecutive_block_errors += 1
            else:
                consecutive_block_errors = 0
            failures.append(
                {
                    **metadata,
                    "transcript_status": "missing",
                    "error": type(exc).__name__,
                    "message": str(exc),
                }
            )
            inventory.append(
                {
                    **metadata,
                    "transcript_status": "missing",
                    "error": type(exc).__name__,
                    "message": str(exc),
                }
            )
            print(f"[{position}/{len(all_videos)}] FAIL {video.title}: {type(exc).__name__}", file=sys.stderr)
            if is_block_error(exc) and consecutive_block_errors >= args.block_threshold:
                halted_due_to_block = True
                print(
                    f"Circuit breaker aberto apos {consecutive_block_errors} bloqueios consecutivos. "
                    "Parando a rodada para evitar agravar o bloqueio.",
                    file=sys.stderr,
                )
                for pending_video in all_videos[position:]:
                    inventory.append(
                        {
                            "video_id": pending_video.video_id,
                            "title": pending_video.title,
                            "published_text": pending_video.published_text,
                            "published_at": pending_video.published_at.isoformat() if pending_video.published_at else None,
                            "url": pending_video.link,
                            "channel_id": context.channel_id,
                            "channel_label": context.channel_label,
                            "transcript_status": "pending",
                            "error": "SkippedAfterBlockCircuit",
                            "message": "Rodada interrompida para evitar agravar bloqueio do YouTube.",
                        }
                    )
                break

        sleep_with_jitter(args.pause_seconds, args.pause_jitter)

    channel_metadata = {
        "channel_id": context.channel_id,
        "channel_label": context.channel_label,
        "output_root": str(channel_root),
        "generated_at": datetime.now(UTC).isoformat(),
        "months": args.months,
        "languages": languages,
        "translate_to": args.translate_to.strip() or None,
        "video_count_selected": len(all_videos),
        "video_count_present": sum(1 for item in inventory if item["transcript_status"] == "present"),
        "video_count_missing": sum(1 for item in inventory if item["transcript_status"] == "missing"),
        "video_count_pending": sum(1 for item in inventory if item["transcript_status"] == "pending"),
        "halted_due_to_block": halted_due_to_block,
    }

    write_json(channel_root / "channel.json", channel_metadata)
    write_json(channel_root / "raw_transcripts_inventory.json", inventory)
    write_json(channel_root / "failures.json", failures)
    write_checklist(channel_root, inventory)

    present = channel_metadata["video_count_present"]
    missing = channel_metadata["video_count_missing"]
    pending = channel_metadata["video_count_pending"]
    print(f"Concluido. {present} presentes, {missing} faltando, {pending} pendentes. Saida em: {channel_root}")
    return 0 if present else 1


if __name__ == "__main__":
    raise SystemExit(main())
