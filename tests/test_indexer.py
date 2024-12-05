import os

import pytest

from generate_index import S3Indexer


def test_init(indexer, bucket_name):
    assert indexer.with_digests

    indexer = S3Indexer(bucket_name=bucket_name, with_digests=False)
    assert indexer.bucket_name == bucket_name
    assert not indexer.with_digests


def test_get_all_wheels(indexer, wheels):
    retrieved_wheels = indexer.get_all_wheels()
    assert wheels == retrieved_wheels


def test_compute_digest(indexer, wheels, s3_fs):
    digest = indexer.compute_digest(wheels[0], write=False)
    assert digest == "3b200e5e581ab8da6bb4810c1277a30b361dfdbb2c109080a3b7a1f121bbcb06"

    assert not s3_fs.exists("s3://{wheels[0]}{indexer.digest_suffix}")

    digest = indexer.compute_digest(wheels[0], write=True)

    assert s3_fs.exists(f"s3://{wheels[0]}{indexer.digest_suffix}")
    with s3_fs.open(f"s3://{wheels[0]}{indexer.digest_suffix}", "r") as fh:
        assert (
            fh.read()
            == "3b200e5e581ab8da6bb4810c1277a30b361dfdbb2c109080a3b7a1f121bbcb06"
        )


@pytest.mark.parametrize("prefix", ["", "prefix"])
def test_write_index(indexer, prefix):
    indexer.write_index(contents="dummy", prefix=prefix)

    if prefix:
        path = f"{indexer.bucket_name}/{prefix}/{indexer.module_name}/index.html"
    else:
        path = f"{indexer.bucket_name}/{indexer.module_name}/index.html"

    with indexer.fs.open(path) as fh:
        index_contents = fh.read().decode()

    assert index_contents == "dummy"


@pytest.mark.parametrize("prefix", ["", "prefix"])
def test_write_index_local(tmp_path, prefix, s3_fs):
    os.chdir(tmp_path)
    indexer = S3Indexer(dry_run=True)
    indexer.write_index(contents="dummy", prefix=prefix)

    if prefix:
        path = f"index_test/{prefix}/{indexer.module_name}/index.html"
    else:
        path = f"index_test/{indexer.module_name}/index.html"

    with open(path) as fh:
        index_contents = fh.read()

    assert index_contents == "dummy"


def test_get_missing_digest(indexer):
    with pytest.raises(FileNotFoundError, match="sha256 digest does not exist:"):
        indexer.get_digest("nonexisting")


def test_compare_digest(indexer, wheels, s3_fs):
    with pytest.raises(FileNotFoundError):
        indexer.compute_digest("nonexistingbucket/nonexisting")
    s3_fs.mkdir("bucket")
    with pytest.raises(FileNotFoundError):
        indexer.compute_digest("bucket/nonexisting")

    file = "bucket/file.whl"
    with s3_fs.open(file, "w") as fh:
        fh.write("dhn")
    with s3_fs.open(f"{file}{indexer.digest_suffix}", "w") as fh:
        fh.write("wrong digest")

    with pytest.raises(ValueError, match="sha256 digest does not match"):
        indexer.compute_digest(file, compare=True)

    # expected_digest = "338fd9894b114dba6235ea4f939c51c7bb7038dd4f79f4c9985c26ae5217e64d"


def test_run(indexer, s3_fs, wheels):
    indexer.run()

    assert s3_fs.exists(f"{indexer.bucket_name}/{indexer.module_name}/index.html")
    assert s3_fs.exists(
        f"{indexer.bucket_name}/nightly/{indexer.module_name}/index.html"
    )

    git_refs = map(
        lambda name: name.split("/")[1],
        filter(lambda name: "nightly" not in name, wheels),
    )
    for ref in git_refs:
        assert s3_fs.exists(
            f"{indexer.bucket_name}/{ref}/{indexer.module_name}/index.html"
        )


def test_get_digest(indexer, wheels):
    digest = indexer.get_digest(wheels[0])
    assert digest == "3b200e5e581ab8da6bb4810c1277a30b361dfdbb2c109080a3b7a1f121bbcb06"


def test_get_all_digests(indexer, wheels):
    digests = indexer.get_all_digests()

    assert digests == list(
        map(
            lambda el: f"{el}{indexer.digest_suffix}",
            wheels,
        )
    )
