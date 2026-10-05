"""Download the pinned MySQL driver without storing it in Git."""
import hashlib
from pathlib import Path
from urllib.request import urlopen

VERSION = "26.7.0"
SHA256 = "69084713593a4aa8d07c383619b9639276f08bccf8faf1c562178147d389b1e1"
URL = f"https://repo.maven.apache.org/maven2/com/mysql/mysql-connector-j/{VERSION}/mysql-connector-j-{VERSION}.jar"


def main():
    target = Path(__file__).resolve().parents[1] / "drivers/mysql-connector-j.jar"
    if target.exists() and hashlib.sha256(target.read_bytes()).hexdigest() == SHA256:
        return
    with urlopen(URL, timeout=60) as response:
        content = response.read()
    if hashlib.sha256(content).hexdigest() != SHA256:
        raise ValueError("MySQL driver checksum mismatch")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".jar.part")
    temporary.write_bytes(content)
    temporary.replace(target)


if __name__ == "__main__":
    main()
