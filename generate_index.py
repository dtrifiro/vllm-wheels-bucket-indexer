import s3fs
import json
import os
from urllib.parse import quote
import logging

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

INDEX_TEST_NAME = "index_test"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

logger = logging.getLogger("indexer")


class S3Indexer:
    cache_file: str = "bucket_dump.json"

    def __init__(self, bucket_name: str = "vllm-wheels", dry_run: bool = True):
        self.bucket_name = bucket_name
        self.fs = s3fs.S3FileSystem()
        self._dry_run = dry_run
        if dry_run:
            logger.warning("Dry run: only generating indexes locally.")

    def get_all_wheels(self) -> list[str]:
        """returns a list of paths of wheels relative to the s3 bucket"""

        if os.path.exists(self.cache_file):
            logger.warning("Using cached data from %s", self.cache_file)

            with open(self.cache_file) as fh:
                return json.load(fh)

        logger.info("Indexing bucket at s3://%s", self.bucket_name)

        wheels = list(
            filter(
                lambda name: name.endswith(".whl"),
                self.fs.find("s3://{self.bucket_name}/"),
            )
        )

        with open(self.cache_file, "w") as fh:
            json.dump(wheels, fh)

        return wheels

    def generate_index(self) -> None:
        wheels = self.get_all_wheels()

        links: list[str] = []
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
                logger.warning("Dry run: making link path absolute")
                wheel_relpath = (
                    f"https://vllm-wheels.s3.us-west-2.amazonaws.com/{wheel_relpath}"
                )

            links.append(
                LINK_TEMPLATE.format(
                    wheel_relative_path=wheel_relpath,
                    wheel_name=wheel_name,
                )
            )

        index_html = DOC_TEMPLATE.format(links="\n".join(links))

        if self._dry_run:
            dir_path = os.path.join(INDEX_TEST_NAME, "vllm")
            os.makedirs(dir_path, exist_ok=True)
            index_path = os.path.join(dir_path, "index.html")
            with open(index_path, "w") as fh:
                fh.write(index_html)
        else:
            index_path = f"s3://{self.bucket_name}/vllm/index.html"
            try:
                with self.fs.open(index_path, "w") as fh:
                    fh.write(index_html)
            except:  # noqa: E722
                logger.exception("Failed to write index for wheel_path=%s", wheel_path)
                return

        logger.info("Wrote index to: %s", index_path)

    def generate_git_refs_indexes(self) -> None:
        logger.info("Generating index for git refs...")
        wheels = self.get_all_wheels()

        nightlies: list[str] = []
        for wheel_path in wheels:
            if "nightly" in wheel_path:
                nightlies.append(wheel_path)
                logger.debug("Skipping nightly wheel for %s", wheel_path)
                continue

            logger.debug("Processing wheel_path=%s", wheel_path)
            try:
                _, git_ref, wheel_name = wheel_path.split("/")
            except ValueError:
                logger.error(
                    "Couldn't extract bucket_name, git_ref and wheel name from wheel_relpath=%s",
                    wheel_path,
                )
                continue

            wheel_relpath = quote(f"{git_ref}/{wheel_name}")
            if self._dry_run:
                logger.warning("Dry run: making link path absolute")
                wheel_relpath = (
                    f"https://vllm-wheels.s3.us-west-2.amazonaws.com/{wheel_relpath}"
                )

            git_ref_link = LINK_TEMPLATE.format(
                wheel_relative_path=wheel_relpath, wheel_name=wheel_name
            )
            index = DOC_TEMPLATE.format(links="\n".join([git_ref_link]))

            if self._dry_run:
                dir_path = os.path.join(INDEX_TEST_NAME, git_ref, "vllm")
                os.makedirs(dir_path, exist_ok=True)
                index_path = os.path.join(dir_path, "index.html")
                with open(index_path, "w") as fh:
                    fh.write(index)
            else:
                index_path = f"s3://{self.bucket_name}/{git_ref}/vllm/index.html"
                try:
                    with self.fs.open(index_path, "w") as fh:
                        fh.write(index)
                except:  # noqa: E722
                    logger.exception(
                        "Failed to write index for wheel_path=%s", wheel_path
                    )
                    continue
            logger.info("Wrote index to: %s", index_path)

        logger.info("Generating index for nightly directory...")
        nightlies_links: list[str] = []
        for wheel_path in nightlies:
            try:
                _, git_ref, wheel_name = wheel_path.split("/")
            except ValueError:
                logger.error(
                    "Couldn't extract bucket_name, git_ref and wheel name from wheel_relpath=%s",
                    wheel_path,
                )
                continue
            if git_ref != "nightly":
                logger.warning(
                    'Found %s expected "nightly" while processing %s. Skipping',
                    git_ref,
                    wheel_path,
                )
                continue

            wheel_relpath = quote(f"{git_ref}/{wheel_name}")
            if self._dry_run:
                wheel_relpath = (
                    f"https://vllm-wheels.s3.us-west-2.amazonaws.com/{wheel_relpath}"
                )

            nightlies_links.append(
                LINK_TEMPLATE.format(
                    wheel_relative_path=wheel_relpath,
                    wheel_name=wheel_name,
                )
            )

        nightly_index = DOC_TEMPLATE.format(links="\n".join(nightlies_links))

        if self._dry_run:
            dir_path = os.path.join(INDEX_TEST_NAME, "nightly", "vllm")
            os.makedirs(dir_path, exist_ok=True)
            index_path = os.path.join(dir_path, "index.html")
            with open(index_path, "w") as fh:
                fh.write(nightly_index)
        else:
            index_path = f"s3://{self.bucket_name}/nightly/vllm/index.html"
            try:
                with self.fs.open(index_path, "w") as fh:
                    fh.write(nightly_index)
            except:  # noqa: E722
                logger.exception("Failed to write index for wheel_path=%s", wheel_path)

        logger.info("Wrote index to: %s", index_path)


def main():
    dry_run = os.getenv("INDEXER_DRY_RUN", "True").lower() != "false"
    indexer = S3Indexer(dry_run=dry_run)

    indexer.generate_index()
    indexer.generate_git_refs_indexes()
    print(
        "\nTo use the index, first serve it using `python -m http.server --directory index_test`, "
        " and then use `--extra-index-url http://localhost:8000/`.\n\n"
        'Use "--extra-index-url http://localhost:8000/<git ref>" to install a specific git ref\n'
        "Installation of dev packages requires the `--pre` pip flag."
    )


if __name__ == "__main__":
    main()
