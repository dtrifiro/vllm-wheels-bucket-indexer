import s3fs
import json
import os
from urllib.parse import quote
import logging
import hashlib

from typing import Callable

DOC_TEMPLATE = """
<!DOCTYPE html>
<html>
    <body>
    <h1>Links for vllm</h1/>
{links}
    </body>
</html>
"""

LINK_TEMPLATE = """<a href="{wheel_relative_path}">{wheel_name}</a><br/>"""

LINK_TEMPLATE_SHA = (
    """<a href="{wheel_relative_path}#{sha256_digest}">{wheel_name}</a><br/>"""
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG if os.getenv("INDEXER_DEBUG") else logging.INFO,
)

logger = logging.getLogger("indexer")


class S3Indexer:
    hash_bs: int = 2 << 22  # 8MiB, tweak as needed
    digest_suffix: str = ".sha256sum"

    def __init__(
        self,
        bucket_name: str = "vllm-wheels",
        module_name: str = "vllm",
        with_digests: bool = True,
        dry_run: bool = False,
    ):
        """
        Arguments:
            - bucket_name: name of the bucket to index
            - absolute_path: use absolute path for wheel links
            - compute_digest: compute sha256 digest for every indexed file
            - write_digest: write sha256 digest back to the bucket
        """
        self.bucket_name = bucket_name
        self.module_name = module_name
        self.fs = s3fs.S3FileSystem()
        self.with_digests = with_digests
        self._dry_run = dry_run

    def get_digest(self, key: str) -> str:
        digest_file = f"{key}{self.digest_suffix}"
        logger.debug("Getting digest: %s", digest_file)

        if not self.fs.exists(digest_file):
            raise FileNotFoundError(
                f"sha256 digest does not exist: {digest_file} is missing"
            )

        with self.fs.open(digest_file, "r") as fh:
            digest = fh.read()

        return digest

    def compute_digest(
        self, key: str, write: bool = True, compare: bool = False
    ) -> str:
        """Compute a SHA256 digest of the file corresponding to the given S3 key

        Arguments:
         - key: the key of the item to retrieve and hash
         - write: write back the digest to the bucket
         - compare: compare the computed digest against the value present in the bucket


        If `compare == True`, raises `KeyError` if the key is missing from the bucket,
        if the values do not match, raises `ValueError`.
        """
        sha256_hash = hashlib.sha256()

        logger.debug("Computing sha256 digest for %s", key)
        with self.fs.open(key, "rb") as fh:
            for byte_block in iter(lambda: fh.read(self.hash_bs), b""):
                sha256_hash.update(byte_block)

        digest = sha256_hash.hexdigest()
        digest_file = f"{key}{self.digest_suffix}"

        if compare:
            current_digest = self.get_digest(key)

            if current_digest != digest:
                raise ValueError(f"sha256 digest does not match for {digest_file}")

        if write and self._dry_run:
            logger.warning("Dry run: not writing digests back to S3")
        elif write:
            with self.fs.open(digest_file, "w") as fh:
                logger.debug("Wrote digest to %s", digest_file)
                fh.write(digest)

        return digest

    def _get_files(self, filter_fn: Callable | None = None) -> list[str]:
        return list(
            filter(
                filter_fn,
                self.fs.find(f"s3://{self.bucket_name}/"),
            )
        )

    def get_all_wheels(self) -> list[str]:
        """returns a list of paths of wheels relative to the s3 bucket"""
        logger.info("Indexing bucket at s3://%s", self.bucket_name)
        wheels = self._get_files(filter_fn=lambda name: name.endswith(".whl"))
        return wheels

    def get_all_digests(self) -> list[str]:
        """returns a list of paths of digests relative to the s3 bucket"""
        logger.info("Indexing digests at s3://%s", self.bucket_name)
        return self._get_files(filter_fn=lambda name: name.endswith(self.digest_suffix))

    def generate_index(
        self, write_locally: bool = True
    ) -> tuple[str, str, dict[str, str]]:
        """Index bucket and return html indexes

        Returns:
            (index, nightlies_index, git_refs_index)

        - index is an html index of all the wheels in the bucket (s3://<bucket>/<module name>/index.html)
        - nightlies_index is an html index of all the nightly wheels in the bucket (s3://<bucket>/nightly/<module name>/index.html)
        - git_refs_index is a dict containing html indexes for each git ref (s3://<bucket>/<git ref>/<module name>/index.html)
        """
        wheels = self.get_all_wheels()

        links: list[str] = []
        nightlies_links: list[str] = []
        git_refs_indexes: dict[str, str] = {}
        if self._dry_run:
            logger.warning("Making link paths absolute")

        for wheel_path in wheels:
            logger.debug("Processing wheel_path=%s", wheel_path)
            try:
                _, git_ref, wheel_name = wheel_path.split("/")
            except ValueError:
                logger.error(
                    "Couldn't extract bucket_name, git_ref and wheel name from wheel_relpath=%s",
                    wheel_path,
                )

            wheel_relpath = quote(f"{git_ref}/{wheel_name}")
            if self._dry_run:
                wheel_relpath = (  # FIXME: this is hardcoded for now
                    f"https://vllm-wheels.s3.us-west-2.amazonaws.com/{wheel_relpath}"
                )

            if self.with_digests:
                try:
                    digest = self.get_digest(wheel_path)
                except FileNotFoundError:
                    digest = self.compute_digest(wheel_path)

                link = LINK_TEMPLATE_SHA.format(
                    wheel_relative_path=wheel_relpath,
                    wheel_name=wheel_name,
                    sha256_digest=digest,
                )
            else:
                link = LINK_TEMPLATE.format(
                    wheel_relative_path=wheel_relpath,
                    wheel_name=wheel_name,
                )

            if "nightly" in wheel_path:
                nightlies_links.append(link)
            else:
                git_refs_indexes[git_ref] = DOC_TEMPLATE.format(links="\n".join([link]))
                links.append(link)

        index = DOC_TEMPLATE.format(links="\n".join(links))
        nightlies_index = DOC_TEMPLATE.format(links="\n".join(nightlies_links))

        return (
            index,
            nightlies_index,
            git_refs_indexes,
        )

    def write_index(
        self,
        contents: str,
        prefix: str = "",
    ) -> None:
        if prefix:
            key = f"{prefix}/{self.module_name}/index.html"
        else:
            key = f"{self.module_name}/index.html"

        if self._dry_run:
            index_path = os.path.join("index_test", key)
            os.makedirs(os.path.dirname(index_path), exist_ok=True)
        else:
            index_path = f"s3://{self.bucket_name}/{key}"
        try:
            if self._dry_run:
                with open(index_path, "w") as fh:
                    fh.write(contents)
            else:
                with self.fs.open(f"s3://{index_path}", "w") as fh:
                    fh.write(contents)
        except:  # noqa: E722
            logger.exception("Failed to write index to=%s", index_path)

    def run(self):
        index_html, nightlies_index, git_refs_indexes = self.generate_index()

        self.write_index(contents=index_html)
        self.write_index(contents=nightlies_index, prefix="nightly")
        for git_ref, index in git_refs_indexes.items():
            self.write_index(contents=index, prefix=git_ref)


def main():
    dry_run = os.getenv("INDEXER_DRY_RUN", "True").lower() != "false"
    S3Indexer(
        dry_run=dry_run,
    ).run()

    if dry_run:
        print(
            "\nTo use the index, first serve it using `python -m http.server --directory index_test`, "
            " and then use `--extra-index-url http://localhost:8000/`.\n\n"
            'Use "--extra-index-url http://localhost:8000/<git ref>" to install a specific git ref\n'
            "Installation of dev packages requires the `--pre` pip flag."
        )


if __name__ == "__main__":
    main()
