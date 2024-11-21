# vllm s3 bucket indexer

Indexes an s3 bucket for wheel files and generates a `pip` compatible index

## Testing

Run the code, might take a while on the first run

```python
python generate_index.py
```

To test out the generated index, start a webserver in another shell:

```bash
python -m http.server --directory index_test
```

This can be now used with pip as an extra index:

```bash
# to install the most recent wheel
pip install --extra-index-url=http://localhost:8000/
# to install the most recent wheel (note the `--pre`)
pip install --pre --extra-index-url=http://localhost:8000/
# to install a specific git revision
git_ref=0057894ef7f8db0d51385aa7254219d7fbd6c784/
pip install --pre --extra-index-url=http://localhost:8000/${git_ref}
```

## Production

Run the indexer with `dry_run=False`. This assumes that your environment is set up for AWS (e.g. `AwS_*` env vars are set and that you have write access to the bucket.

```bash
INDEXER_DRY_RUN=False python generate_index.py
```
