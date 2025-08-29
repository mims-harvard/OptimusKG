# hyperfine \
#   --prepare "rm -rf data/bronze" \
#   --warmup 1 \
#   --min-runs 3 \
#   --export-markdown results.md \
#   --export-json results.json \
#   --parameter-list runner ParallelRunner,SequentialRunner \
#   "uv run kedro run --pipeline=bronze --runner={runner} --async"

hyperfine \
  --prepare "rm -rf data/bronze && rm -rf data/silver && rm -rf data/gold" \
  --warmup 1 \
  --min-runs 3 \
  --export-markdown results.md \
  --export-json results.json \
  --parameter-list runner ParallelRunner,SequentialRunner \
  "uv run kedro run --to-nodes=gold.pg_export --runner={runner} --async"