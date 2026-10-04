FROM golang:1.24

WORKDIR /app

RUN git config --system --add safe.directory /app

# Documents (render_pdf, make_slides): pandoc and typst, official release
# binaries pinned by version and SHA256 (Debian's pandoc is too old for the
# typst writer the tools rely on). A version changed here takes its sums with
# it. Open Sans is the slides' face; typst embeds the rest (Libertinus Serif,
# New Computer Modern, DejaVu Sans Mono).
ARG PANDOC_VERSION=3.12
ARG TYPST_VERSION=0.15.1
ARG TARGETARCH
RUN set -eu; \
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "$arch" in \
      amd64) typst_arch=x86_64; \
             pandoc_sum=67d7d011fed8c8543306022b985b9b2499ab9b74818df91d8727c7e9ebc5ba06; \
             typst_sum=a6d077d0a95eed5a2eba715b2dae06be954f624ccbf85758a03f389ded33118c ;; \
      arm64) typst_arch=aarch64; \
             pandoc_sum=6cefcf7100e23a99447c26f89d1ff5b253f3407fcef99a9e27ae06f3ed16cb82; \
             typst_sum=5aa8d74a3d906e60ea12a66ac2f37f8eef1b14cbad7182a745e393a10c23dcee ;; \
      *) echo "no pandoc and typst pinned for $arch" >&2; exit 1 ;; \
    esac; \
    apt-get update; \
    apt-get install -y --no-install-recommends fonts-open-sans xz-utils; \
    cd /tmp; \
    curl -fsSL -o pandoc.tar.gz "https://github.com/jgm/pandoc/releases/download/${PANDOC_VERSION}/pandoc-${PANDOC_VERSION}-linux-${arch}.tar.gz"; \
    echo "$pandoc_sum  pandoc.tar.gz" | sha256sum -c -; \
    tar -xzf pandoc.tar.gz -C /usr/local/bin --no-same-owner --strip-components=2 "pandoc-${PANDOC_VERSION}/bin/pandoc"; \
    curl -fsSL -o typst.tar.xz "https://github.com/typst/typst/releases/download/v${TYPST_VERSION}/typst-${typst_arch}-unknown-linux-musl.tar.xz"; \
    echo "$typst_sum  typst.tar.xz" | sha256sum -c -; \
    tar -xJf typst.tar.xz -C /usr/local/bin --no-same-owner --strip-components=1 "typst-${typst_arch}-unknown-linux-musl/typst"; \
    rm pandoc.tar.gz typst.tar.xz; \
    pandoc --version >/dev/null && typst --version >/dev/null; \
    apt-get purge -y --auto-remove xz-utils; \
    rm -rf /var/lib/apt/lists/*

# The typst packages a document may import, pinned the same way: typst never
# downloads one at run time (the tools point its package path and cache here,
# where the user they run as cannot write, and give it no proxy that leads
# anywhere). touying is the slides' framework; uniwarn is its dependency.
ENV TYPST_PACKAGES=/usr/local/share/typst/packages
RUN set -eu; \
    for pkg in \
      touying:0.8.0:7fe9eeeecc5614f21ee64ffe95c141d36ab1ae0b4ec961e6eabf13ab875ef5c1 \
      uniwarn:0.1.1:68247432dab1b165c16055411c0f0ec154a63d7fcf5ef0f65906269695d368ea; do \
      name="${pkg%%:*}"; rest="${pkg#*:}"; version="${rest%%:*}"; sum="${rest#*:}"; \
      dir="$TYPST_PACKAGES/preview/$name/$version"; \
      curl -fsSL -o /tmp/pkg.tar.gz "https://packages.typst.org/preview/${name}-${version}.tar.gz"; \
      echo "$sum  /tmp/pkg.tar.gz" | sha256sum -c -; \
      mkdir -p "$dir"; tar -xzf /tmp/pkg.tar.gz -C "$dir" --no-same-owner; rm /tmp/pkg.tar.gz; \
    done; \
    chmod -R u=rwX,go=rX "$TYPST_PACKAGES"

# The user exec runs its commands as. The worker runs as root, and a command
# run as root would read the platform's credentials back from the worker's
# /proc/<pid>/environ, whatever its own environment holds. The worker refuses
# exec as root without RUN_AS_UID, and gives this user the workspace at
# startup.
RUN groupadd --gid 10001 agent-run \
 && useradd --uid 10001 --gid 10001 --no-log-init --create-home --home-dir /home/agent-run \
      --shell /usr/sbin/nologin agent-run
ENV RUN_AS_UID=10001

CMD ["sh", "-c", "rm -f ./tmp/main && go build -buildvcs=false -o ./tmp/main ./cmd/agent && exec ./tmp/main dev"]
