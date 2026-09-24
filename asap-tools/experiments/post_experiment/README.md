# post_experiment

- `single_experiment/` — analyze one experiment (its `baseline` and `sketchdb` modes). Prints stats, `--machine-readable` JSON, optional plots. Cost and latency definitions live here.
- `multi_experiment/` — sweep across experiments and produce comparison figures. Get numbers from `single_experiment/` scripts or `lib/`.
- `lib/` — shared loaders (`results_loader.py`).
- `debug/` — one-off inspection tools.

Run scripts from any directory; they locate `constants.py` and sibling scripts relative to their own path.
