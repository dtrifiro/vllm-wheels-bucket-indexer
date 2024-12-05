import pytest
import json
import s3fs


from generate_index import S3Indexer


@pytest.fixture(scope="session")
def bucket_name():
    return "vllm-wheels"


@pytest.fixture(scope="session")
def wheels():
    with open("tests/data/bucket_dump.json") as fh:
        wheels = json.load(fh)

    return wheels


@pytest.fixture(scope="session")
def s3_fake_creds_file(monkeypatch_session: pytest.MonkeyPatch) -> None:  # type: ignore[misc]
    # https://github.com/spulec/moto#other-caveats
    import pathlib

    aws_dir = pathlib.Path("~").expanduser() / ".aws"
    aws_dir.mkdir(exist_ok=True)

    aws_creds = aws_dir / "credentials"
    initially_exists = aws_creds.exists()

    if not initially_exists:
        aws_creds.touch()

    try:
        with monkeypatch_session.context() as m:
            try:
                m.delenv("AWS_PROFILE")
            except KeyError:
                pass
            m.setenv("AWS_ACCESS_KEY_ID", "pytest-servers")
            m.setenv("AWS_SECRET_ACCESS_KEY", "pytest-servers")
            m.setenv("AWS_SECURITY_TOKEN", "pytest-servers")
            m.setenv("AWS_SESSION_TOKEN", "pytest-servers")
            m.setenv("AWS_DEFAULT_REGION", "us-east-1")
            yield
    finally:
        if aws_creds.exists() and not initially_exists:
            aws_creds.unlink()


@pytest.fixture(scope="session")
def s3_fs(
    s3_fake_creds_file,
    s3_server,  # pytest-servers fixture, mocks an s3 server using moto
):
    """S3FileSystem with the local moto instance"""
    return s3fs.S3FileSystem(
        endpoint_url=s3_server["endpoint_url"],
    )


@pytest.fixture(scope="session")
def mocked_wheels_bucket(
    bucket_name,
    wheels,
    s3_fs,
):
    s3_fs.mkdir(f"s3://{bucket_name}")
    for wheel in wheels:
        with s3_fs.open(f"s3://{wheel}", "w") as fh:
            fh.write(wheel)  # write wheel name as dummy contents


@pytest.fixture(scope="session")
def indexer(mocked_wheels_bucket, s3_fs, bucket_name):
    """Indexer pointing to the local moto instance"""
    indexer = S3Indexer(
        bucket_name=bucket_name,
        module_name="vllm",
    )
    indexer.fs = s3_fs

    yield indexer
