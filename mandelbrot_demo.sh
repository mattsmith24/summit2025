#!/bin/bash

uv run mandelbrot_host.py
for i in {1..10}; do
    uv run mandelbrot_worker.py &
done
uv run mandelbrot_render.py &
wait
echo done