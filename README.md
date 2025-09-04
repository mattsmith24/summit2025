# WARNING: AI Slop

## Installation

1. Install Python dependencies:
   ```bash
    sudo apt install libportaudio2
   ```

## Run It

```
docker run -p 6379:6379 -d redis
uv run music_player.py --redis-type=poll_stream
```

In another terminal:

```
uv run redis_publisher.py songs/take_on_me.csv --redis-type stream --speed=3
```

# Troubleshooting

If you get the error:

```
qt.qpa.plugin: From 6.5.0, xcb-cursor0 or libxcb-cursor0 is needed to load the Qt xcb platform plugin.
qt.qpa.plugin: Could not load the Qt platform plugin "xcb" in "" even though it was found.
This application failed to start because no Qt platform plugin could be initialized. Reinstalling the application may fix this problem.

Available platform plugins are: eglfs, minimalegl, vkkhrdisplay, wayland, minimal, vnc, xcb, wayland-egl, linuxfb, offscreen.
```

Then install `libxcb-cursor-dev`

```bash
sudo apt install libxcb-cursor-dev
```
