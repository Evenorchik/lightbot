import logging
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from PIL import Image, ImageDraw, ImageFont

import utils

logger = logging.getLogger(__name__)


def render_schedule_image(
    schedule_date: str,
    group_code: str,
    on_intervals: list[str],
    off_intervals: list[str],
    now_dt: datetime,
    tz_name: str = "Europe/Kyiv",
) -> str:
    """
    Сгенерировать PNG изображение графика (портретный формат, 2 полосы по 12 часов).

    Args:
        schedule_date: дата в формате DD.MM.YYYY
        group_code: код группы (X.Y)
        on_intervals: список интервалов "HH:MM–HH:MM" когда есть свет (может не использоваться, оставлено для совместимости)
        off_intervals: список интервалов "HH:MM–HH:MM" когда нет света
        now_dt: текущее время (желательно timezone-aware)
        tz_name: название таймзоны

    Returns:
        путь к созданному PNG файлу
    """
    # ---------------------------
    # Phone portrait layout
    # ---------------------------
    W, H = 720, 980
    OUT_PAD = 24

    # Card geometry
    CARD_BG = (255, 255, 255, 255)
    APP_BG = (242, 243, 247, 255)

    CARD_RADIUS = 22
    CARD_SHADOW = (0, 0, 0, 22)

    # Header card
    header_h = 120

    # Two band cards
    band_h = 290
    band_gap = 18

    # Legend card
    legend_h = 110

    # vertical flow
    y = OUT_PAD
    header_y = y
    y += header_h + 14

    band1_y = y
    y += band_h + band_gap
    band2_y = y
    y += band_h + 14

    legend_y = y

    # inside card padding
    PAD = 20

    # ---------------------------
    # Colors (soft, modern)
    # ---------------------------
    TEXT = (25, 25, 28, 255)
    MUTED = (110, 110, 118, 255)
    BORDER = (230, 231, 236, 255)

    ON = (76, 175, 80, 255)        # green
    OFF = (244, 67, 54, 255)       # red

    TICK_MINOR = (255, 255, 255, 90)
    TICK_MAJOR = (255, 255, 255, 140)

    MARKER = (20, 20, 22, 255)
    MARKER_OUTLINE = (255, 255, 255, 255)

    BUBBLE_BG = (255, 255, 255, 245)
    BUBBLE_BORDER = (200, 201, 206, 255)
    BUBBLE_SHADOW = (0, 0, 0, 26)

    # ---------------------------
    # Helpers
    # ---------------------------
    def load_font(preferred_paths: list[str], size: int) -> ImageFont.FreeTypeFont:
        for p in preferred_paths:
            try:
                if os.path.exists(p):
                    return ImageFont.truetype(p, size)
            except Exception:
                pass
        for p in [
            r"C:\Windows\Fonts\arial.ttf",
            r"C:\Windows\Fonts\segoeui.ttf",
            r"C:\Windows\Fonts\calibri.ttf",
        ]:
            try:
                if os.path.exists(p):
                    return ImageFont.truetype(p, size)
            except Exception:
                pass
        return ImageFont.load_default()

    def text_size(draw_: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> tuple[int, int]:
        bbox = draw_.textbbox((0, 0), text, font=font)
        return bbox[2] - bbox[0], bbox[3] - bbox[1]

    def clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, v))

    def normalize_tz(name: str) -> str:
        return "Europe/Kyiv" if name == "Europe/Uzhgorod" else name

    def get_tz(name: str) -> timezone:
        key = normalize_tz(name)
        try:
            return ZoneInfo(key)
        except ZoneInfoNotFoundError:
            try:
                return ZoneInfo("Europe/Kyiv")
            except ZoneInfoNotFoundError:
                return timezone.utc

    def rounded_card(base_img: Image.Image, box: tuple[int, int, int, int], radius: int) -> None:
        """Draw card with soft shadow + rounded rect."""
        x1, y1, x2, y2 = box

        # Shadow layer
        shadow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        sd = ImageDraw.Draw(shadow)
        # a little offset for depth
        sd.rounded_rectangle((x1, y1 + 6, x2, y2 + 6), radius=radius, fill=CARD_SHADOW)
        base_img.alpha_composite(shadow)

        # Card itself
        d = ImageDraw.Draw(base_img)
        d.rounded_rectangle(box, radius=radius, fill=CARD_BG, outline=BORDER, width=1)

    def intersect_interval(a1: int, a2: int, b1: int, b2: int) -> tuple[int, int] | None:
        """Intersection of [a1,a2] with [b1,b2]. Treat as half-open [start,end)."""
        s = max(a1, b1)
        e = min(a2, b2)
        if e <= s:
            return None
        return s, e

    def parse_off_intervals() -> list[tuple[int, int]]:
        parsed: list[tuple[int, int]] = []
        for it in off_intervals:
            try:
                s, e = utils.parse_interval(it)
                # normalize weird intervals if needed
                s = max(0, min(1440, s))
                e = max(0, min(1440, e))
                if e > s:
                    parsed.append((s, e))
            except Exception:
                continue
        return parsed

    # ---------------------------
    # TZ + now
    # ---------------------------
    tz = get_tz(tz_name)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    now_local = now_dt.astimezone(tz)

    try:
        schedule_date_obj = utils.parse_date_ddmmyyyy(schedule_date)
    except Exception:
        schedule_date_obj = now_local.date()

    show_marker = (schedule_date_obj == now_local.date())
    now_min = now_local.hour * 60 + now_local.minute

    off_parsed = parse_off_intervals()

    # ---------------------------
    # Canvas
    # ---------------------------
    img = Image.new("RGBA", (W, H), APP_BG)
    draw = ImageDraw.Draw(img)

    # ---------------------------
    # Fonts
    # ---------------------------
    assets_regular = [
        os.path.join("assets", "DejaVuSans.ttf"),
        os.path.join("assets", "Inter-Regular.ttf"),
    ]
    assets_bold = [
        os.path.join("assets", "DejaVuSans-Bold.ttf"),
        os.path.join("assets", "Inter-SemiBold.ttf"),
        os.path.join("assets", "Inter-Bold.ttf"),
    ]
    font_title = load_font(assets_bold, 30)
    font_subtitle = load_font(assets_regular, 16)
    font_band_title = load_font(assets_bold, 18)
    font_scale = load_font(assets_regular, 15)
    font_legend = load_font(assets_regular, 15)
    font_bubble = load_font(assets_bold, 24)

    # ---------------------------
    # Cards
    # ---------------------------
    header_box = (OUT_PAD, header_y, W - OUT_PAD, header_y + header_h)
    band1_box = (OUT_PAD, band1_y, W - OUT_PAD, band1_y + band_h)
    band2_box = (OUT_PAD, band2_y, W - OUT_PAD, band2_y + band_h)
    legend_box = (OUT_PAD, legend_y, W - OUT_PAD, legend_y + legend_h)

    rounded_card(img, header_box, CARD_RADIUS)
    rounded_card(img, band1_box, CARD_RADIUS)
    rounded_card(img, band2_box, CARD_RADIUS)
    rounded_card(img, legend_box, CARD_RADIUS)

    draw = ImageDraw.Draw(img)

    # ---------------------------
    # Header content
    # ---------------------------
    title = f"Група {group_code}"
    subtitle = f"Графік на {schedule_date}"

    tx = header_box[0] + PAD
    ty = header_box[1] + 22
    draw.text((tx, ty), title, fill=TEXT, font=font_title)
    draw.text((tx, ty + 44), subtitle, fill=MUTED, font=font_subtitle)

    # Optional: small "timezone" hint
    tz_hint = tz.key if hasattr(tz, "key") else str(tz)
    draw.text((tx, header_box[3] - 28), f"Часова зона: {tz_hint}", fill=(140, 140, 148, 255), font=font_subtitle)

    # ---------------------------
    # Band renderer (12h)
    # ---------------------------
    def draw_band(card_box: tuple[int, int, int, int], band_start: int, band_end: int, label: str) -> None:
        """
        Draw 12-hour band inside card.
        band_start/band_end in minutes, e.g. 0..720 or 720..1440
        """
        cx1, cy1, cx2, cy2 = card_box

        # Всегда работаем через текущий draw верхнего уровня (НЕ переопределяем draw!)
        # Если нужно получить свежий объект после alpha_composite — используем локальную переменную d.

        # Inner layout
        inner_x1 = cx1 + PAD
        inner_x2 = cx2 - PAD
        inner_w = inner_x2 - inner_x1

        # Band title
        draw.text((inner_x1, cy1 + 18), label, fill=TEXT, font=font_band_title)

        # Timeline geometry inside band card
        labels_y = cy1 + 54
        timeline_y = cy1 + 82
        timeline_h = 86

        timeline_x = inner_x1
        timeline_w = inner_w
        radius = 20

        tl_box = (timeline_x, timeline_y, timeline_x + timeline_w, timeline_y + timeline_h)

        # Rounded mask for clipping
        mask = Image.new("L", (W, H), 0)
        md = ImageDraw.Draw(mask)
        md.rounded_rectangle(tl_box, radius=radius, fill=255)

        # Segments layer
        seg = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        sd = ImageDraw.Draw(seg)

        # base ON fill
        sd.rectangle(tl_box, fill=ON)

        # helpers for mapping minutes in this band
        def minute_to_x(m: int) -> float:
            m = max(band_start, min(band_end, m))
            span = (band_end - band_start)
            rel = (m - band_start) / float(span)
            return timeline_x + rel * timeline_w

        # OFF overlays clipped to this band
        for s, e in off_parsed:
            inter = intersect_interval(s, e, band_start, band_end)
            if not inter:
                continue
            ss, ee = inter

            x1 = minute_to_x(ss)
            x2 = minute_to_x(ee)
            if x2 > x1:
                sd.rectangle((x1, timeline_y, x2, timeline_y + timeline_h), fill=OFF)

        clipped = Image.composite(seg, Image.new("RGBA", (W, H), (0, 0, 0, 0)), mask)
        img.alpha_composite(clipped)

        # Border (берём свежий d)
        d = ImageDraw.Draw(img)
        d.rounded_rectangle(tl_box, radius=radius, outline=BORDER, width=2)

        # Hour ticks (each hour) inside timeline
        ticks = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        td = ImageDraw.Draw(ticks)

        tick_top = timeline_y + 12
        tick_bot = timeline_y + timeline_h - 12

        start_hour = band_start // 60
        end_hour = band_end // 60
        for hour in range(start_hour, end_hour + 1):
            m = hour * 60
            if m < band_start or m > band_end:
                continue
            x = minute_to_x(m)
            major = (hour % 3 == 0) or (hour == start_hour) or (hour == end_hour)
            color = TICK_MAJOR if major else TICK_MINOR
            width = 2 if major else 1
            td.line([(x, tick_top), (x, tick_bot)], fill=color, width=width)

        ticks_clipped = Image.composite(ticks, Image.new("RGBA", (W, H), (0, 0, 0, 0)), mask)
        img.alpha_composite(ticks_clipped)

        # Labels (every 3 hours, include band edges)
        labels = []
        minutes = []
        for mm in range(band_start, band_end + 1, 180):
            labels.append(f"{mm // 60:02d}:00" if mm < 1440 else "24:00")
            minutes.append(mm)
        if minutes and minutes[-1] != band_end:
            minutes.append(band_end)
            labels.append(f"{band_end // 60:02d}:00" if band_end < 1440 else "24:00")

        for i, (lab, mm) in enumerate(zip(labels, minutes)):
            x = minute_to_x(mm)
            tw, th = text_size(draw, lab, font_scale)

            if i == 0:
                tx = timeline_x
            elif i == len(labels) - 1:
                tx = timeline_x + timeline_w - tw
            else:
                tx = x - tw / 2

            tx = clamp(tx, timeline_x, timeline_x + timeline_w - tw)
            draw.text((tx, labels_y), lab, fill=MUTED, font=font_scale)

        # "Now" marker in this band
        if show_marker and (band_start <= now_min <= band_end):
            x = minute_to_x(now_min)
            x = clamp(x, timeline_x, timeline_x + timeline_w)
            y_mid = timeline_y + timeline_h / 2

            time_str = now_local.strftime("%H:%M")
            tw, th = text_size(draw, time_str, font_bubble)

            pad_x = 12
            pad_y = 7
            bubble_w = tw + 2 * pad_x
            bubble_h = th + 2 * pad_y

            bubble_y = timeline_y - 16 - bubble_h
            min_bubble_y = cy1 + 54
            bubble_y = max(bubble_y, min_bubble_y)

            bubble_x = x - bubble_w / 2
            bubble_x = clamp(bubble_x, cx1 + PAD, cx2 - PAD - bubble_w)

            draw.line([(x, y_mid), (x, bubble_y + bubble_h)], fill=(0, 0, 0, 70), width=1)

            # bubble shadow
            bubble_shadow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            bsd = ImageDraw.Draw(bubble_shadow)
            bsd.rounded_rectangle(
                (bubble_x + 1, bubble_y + 3, bubble_x + bubble_w + 1, bubble_y + bubble_h + 3),
                radius=14,
                fill=BUBBLE_SHADOW,
            )
            img.alpha_composite(bubble_shadow)

            # bubble + dot (свежий d после alpha_composite)
            d = ImageDraw.Draw(img)
            d.rounded_rectangle(
                (bubble_x, bubble_y, bubble_x + bubble_w, bubble_y + bubble_h),
                radius=14,
                fill=BUBBLE_BG,
                outline=BUBBLE_BORDER,
                width=1,
            )
            d.text((bubble_x + pad_x, bubble_y + pad_y), time_str, fill=MARKER, font=font_bubble)

            r = 7
            d.ellipse((x - r - 2, y_mid - r - 2, x + r + 2, y_mid + r + 2), fill=MARKER_OUTLINE)
            d.ellipse((x - r, y_mid - r, x + r, y_mid + r), fill=MARKER)

        # hint under timeline
        hint_y = timeline_y + timeline_h + 16
        hint = "Зелений — є світло • Червоний — немає світла"
        draw.text((inner_x1, hint_y), hint, fill=(150, 150, 158, 255), font=font_subtitle)

    # Draw bands
    draw_band(band1_box, 0, 720, "00:00 — 12:00")
    draw_band(band2_box, 720, 1440, "12:00 — 24:00")

    # ---------------------------
    # Legend card (clean)
    # ---------------------------
    lx1, ly1, lx2, ly2 = legend_box
    lxi = lx1 + PAD
    lyi = ly1 + 22

    draw.text((lxi, lyi), "Легенда", fill=TEXT, font=font_band_title)

    box = 16
    gap = 10
    row_y = lyi + 36

    # ON
    draw.rounded_rectangle((lxi, row_y, lxi + box, row_y + box), radius=4, fill=ON)
    draw.text((lxi + box + gap, row_y - 2), "Є світло", fill=TEXT, font=font_legend)

    # OFF
    x2 = lxi + 220
    draw.rounded_rectangle((x2, row_y, x2 + box, row_y + box), radius=4, fill=OFF)
    draw.text((x2 + box + gap, row_y - 2), "Немає світла", fill=TEXT, font=font_legend)

    # ---------------------------
    # Save
    # ---------------------------
    os.makedirs("tmp", exist_ok=True)
    ts = int(now_dt.timestamp())
    filename = f"schedule_{group_code.replace('.', '_')}_{ts}.png"
    filepath = os.path.join("tmp", filename)

    img.convert("RGB").save(filepath, "PNG")
    logger.debug(f"Изображение сохранено: {filepath}")
    return filepath
